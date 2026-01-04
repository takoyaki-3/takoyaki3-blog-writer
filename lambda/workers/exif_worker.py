import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

import boto3

ddb = boto3.resource("dynamodb")
location = boto3.client("location")
s3 = boto3.client("s3")

UPLOADS_TABLE = os.environ.get("UPLOADS_TABLE", "")
METADATA_TABLE = os.environ.get("METADATA_TABLE", "")
PLACE_INDEX_NAME = os.environ.get("PLACE_INDEX_NAME", "")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _reverse_geocode(lat: float, lng: float) -> Optional[Dict[str, Any]]:
    if not PLACE_INDEX_NAME:
        return None
    try:
        response = location.search_place_index_for_position(
            IndexName=PLACE_INDEX_NAME,
            Position=[lng, lat],
            MaxResults=1,
        )
    except Exception:
        return None

    results = response.get("Results") or []
    if not results:
        return None

    place = results[0].get("Place") or {}
    return {
        "country": place.get("Country"),
        "prefecture": place.get("Region"),
        "city": place.get("Municipality"),
        "label": place.get("Label"),
    }


def handler(event: Dict[str, Any], _context: Any) -> None:
    if not UPLOADS_TABLE or not METADATA_TABLE:
        return

    uploads_table = ddb.Table(UPLOADS_TABLE)
    metadata_table = ddb.Table(METADATA_TABLE)

    for record in event.get("Records", []):
        try:
            payload = json.loads(record.get("body") or "{}")
        except json.JSONDecodeError:
            continue

        upload_id = payload.get("upload_id")
        if not isinstance(upload_id, str) or not upload_id:
            continue

        bucket = payload.get("bucket")
        key = payload.get("key")

        gps = payload.get("gps") if isinstance(payload.get("gps"), dict) else {}
        gps_lat = gps.get("lat") if _is_number(gps.get("lat")) else None
        gps_lng = gps.get("lng") if _is_number(gps.get("lng")) else None

        reverse_geocode = (
            _reverse_geocode(gps_lat, gps_lng) if gps_lat is not None and gps_lng is not None else None
        )

        now = datetime.utcnow().isoformat() + "Z"
        item: Dict[str, Any] = {
            "upload_id": upload_id,
            "updated_at": now,
        }

        if isinstance(bucket, str) and isinstance(key, str):
            try:
                head = s3.head_object(Bucket=bucket, Key=key)
            except Exception as exc:
                print(f"Failed to head object {bucket}/{key}: {exc}")
            else:
                item["object_bucket"] = bucket
                item["object_key"] = key
                item["s3_uri"] = f"s3://{bucket}/{key}"
                content_type = head.get("ContentType")
                if isinstance(content_type, str):
                    item["content_type"] = content_type
                content_length = head.get("ContentLength")
                if isinstance(content_length, int):
                    item["content_length"] = content_length
                last_modified = head.get("LastModified")
                if last_modified is not None:
                    try:
                        item["last_modified"] = last_modified.isoformat()
                    except Exception:
                        pass

        if isinstance(payload.get("datetime_original"), str):
            item["datetime_original"] = payload.get("datetime_original")
        if gps_lat is not None and gps_lng is not None:
            item["gps_lat"] = gps_lat
            item["gps_lng"] = gps_lng
        if isinstance(payload.get("camera_make"), str):
            item["camera_make"] = payload.get("camera_make")
        if isinstance(payload.get("camera_model"), str):
            item["camera_model"] = payload.get("camera_model")
        if reverse_geocode:
            item["reverse_geocode"] = reverse_geocode

        metadata_table.put_item(Item=item)

        uploads_table.update_item(
            Key={"upload_id": upload_id},
            UpdateExpression="SET #status = :status, updated_at = :updated_at",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "metadata_ready",
                ":updated_at": now,
            },
        )
