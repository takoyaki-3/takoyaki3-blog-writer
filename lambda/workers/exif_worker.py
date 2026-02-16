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

_EXIF_RANGE_BYTES = 256 * 1024 - 1


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _as_text(value: Any) -> Optional[str]:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


def _read_int(data: bytes, offset: int, size: int, byte_order: str, signed: bool = False) -> Optional[int]:
    if offset < 0 or offset + size > len(data):
        return None
    return int.from_bytes(data[offset : offset + size], byte_order, signed=signed)


def _read_rationals(data: bytes, count: int, byte_order: str, signed: bool = False) -> list:
    values = []
    for index in range(count):
        base = index * 8
        num = _read_int(data, base, 4, byte_order, signed=signed)
        den = _read_int(data, base + 4, 4, byte_order, signed=signed)
        if num is None or den in (None, 0):
            values.append(None)
        else:
            values.append(num / den)
    return values


def _parse_tiff_value(value_type: int, count: int, data: bytes, byte_order: str) -> Any:
    if value_type == 2:
        try:
            text = data.split(b"\x00", 1)[0].decode("ascii", errors="ignore")
        except Exception:
            return None
        return text.strip()
    if value_type == 3:
        values = [
            _read_int(data, offset, 2, byte_order)
            for offset in range(0, len(data), 2)
        ]
    elif value_type == 4:
        values = [
            _read_int(data, offset, 4, byte_order)
            for offset in range(0, len(data), 4)
        ]
    elif value_type == 5:
        values = _read_rationals(data, count, byte_order, signed=False)
    elif value_type == 7:
        values = list(data)
    elif value_type == 9:
        values = [
            _read_int(data, offset, 4, byte_order, signed=True)
            for offset in range(0, len(data), 4)
        ]
    elif value_type == 10:
        values = _read_rationals(data, count, byte_order, signed=True)
    else:
        return None

    if count == 1:
        return values[0] if values else None
    return values


def _read_tiff_value(
    data: bytes, value_type: int, count: int, value_offset: bytes, byte_order: str
) -> Any:
    type_sizes = {1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 7: 1, 9: 4, 10: 8}
    unit = type_sizes.get(value_type)
    if unit is None or count is None:
        return None
    total = unit * count
    if total <= 4:
        value_data = value_offset[:total]
    else:
        offset = int.from_bytes(value_offset, byte_order)
        if offset < 0 or offset + total > len(data):
            return None
        value_data = data[offset : offset + total]
    return _parse_tiff_value(value_type, count, value_data, byte_order)


def _parse_ifd(data: bytes, offset: int, byte_order: str) -> Dict[int, Any]:
    count = _read_int(data, offset, 2, byte_order)
    if count is None:
        return {}
    cursor = offset + 2
    tags: Dict[int, Any] = {}
    for _ in range(count):
        if cursor + 12 > len(data):
            break
        tag = _read_int(data, cursor, 2, byte_order)
        value_type = _read_int(data, cursor + 2, 2, byte_order)
        value_count = _read_int(data, cursor + 4, 4, byte_order)
        value_offset = data[cursor + 8 : cursor + 12]
        if tag is not None and value_type is not None and value_count is not None:
            tags[tag] = _read_tiff_value(data, value_type, value_count, value_offset, byte_order)
        cursor += 12
    return tags


def _gps_to_decimal(values: Any, ref: Optional[str]) -> Optional[float]:
    if not isinstance(values, list) or len(values) < 3:
        return None
    try:
        deg, minutes, seconds = values[:3]
    except Exception:
        return None
    if not _is_number(deg) or not _is_number(minutes) or not _is_number(seconds):
        return None
    decimal = float(deg) + float(minutes) / 60.0 + float(seconds) / 3600.0
    if isinstance(ref, str) and ref.upper() in ("S", "W"):
        decimal *= -1
    return decimal


def _extract_exif_from_jpeg(data: bytes) -> Dict[str, Any]:
    if len(data) < 4 or data[0:2] != b"\xFF\xD8":
        return {}

    idx = 2
    exif_payload = None
    while idx + 4 <= len(data):
        if data[idx] != 0xFF:
            break
        marker = data[idx + 1]
        if marker in (0xD9, 0xDA):
            break
        segment_length = _read_int(data, idx + 2, 2, "big")
        if segment_length is None or segment_length < 2:
            break
        segment_start = idx + 4
        segment_end = idx + 2 + segment_length
        if segment_end > len(data):
            break
        if marker == 0xE1 and data[segment_start : segment_start + 6] == b"Exif\x00\x00":
            exif_payload = data[segment_start + 6 : segment_end]
            break
        idx = segment_end

    if not exif_payload or len(exif_payload) < 8:
        return {}

    byte_order = "little" if exif_payload[:2] == b"II" else "big" if exif_payload[:2] == b"MM" else None
    if not byte_order:
        return {}
    if _read_int(exif_payload, 2, 2, byte_order) != 42:
        return {}

    ifd0_offset = _read_int(exif_payload, 4, 4, byte_order)
    if ifd0_offset is None:
        return {}

    ifd0 = _parse_ifd(exif_payload, ifd0_offset, byte_order)

    make = _as_text(ifd0.get(0x010F))
    model = _as_text(ifd0.get(0x0110))

    exif_offset = ifd0.get(0x8769)
    exif_ifd = _parse_ifd(exif_payload, exif_offset, byte_order) if isinstance(exif_offset, int) else {}
    datetime_original = _as_text(exif_ifd.get(0x9003)) or _as_text(exif_ifd.get(0x0132))

    gps_offset = ifd0.get(0x8825)
    gps_ifd = _parse_ifd(exif_payload, gps_offset, byte_order) if isinstance(gps_offset, int) else {}
    lat_ref = _as_text(gps_ifd.get(0x0001))
    lat_values = gps_ifd.get(0x0002)
    lng_ref = _as_text(gps_ifd.get(0x0003))
    lng_values = gps_ifd.get(0x0004)

    gps_lat = _gps_to_decimal(lat_values, lat_ref)
    gps_lng = _gps_to_decimal(lng_values, lng_ref)

    result: Dict[str, Any] = {}
    if make:
        result["camera_make"] = make
    if model:
        result["camera_model"] = model
    if datetime_original:
        result["datetime_original"] = datetime_original
    if _is_number(gps_lat) and _is_number(gps_lng):
        result["gps_lat"] = gps_lat
        result["gps_lng"] = gps_lng
    return result


def _should_extract_exif(content_type: Optional[str], key: Optional[str]) -> bool:
    if isinstance(content_type, str) and "jpeg" in content_type.lower():
        return True
    if isinstance(key, str) and key.lower().endswith((".jpg", ".jpeg")):
        return True
    return False


def _load_exif_data(bucket: str, key: str, content_type: Optional[str]) -> Dict[str, Any]:
    if not _should_extract_exif(content_type, key):
        return {}
    try:
        response = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{_EXIF_RANGE_BYTES}")
        body = response.get("Body")
        if body is None:
            return {}
        data = body.read()
    except Exception as exc:
        print(f"Failed to load EXIF data from {bucket}/{key}: {exc}")
        return {}
    return _extract_exif_from_jpeg(data or b"")


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

        now = datetime.utcnow().isoformat() + "Z"
        item: Dict[str, Any] = {
            "upload_id": upload_id,
            "updated_at": now,
        }

        exif_data: Dict[str, Any] = {}
        if isinstance(bucket, str) and isinstance(key, str):
            content_type = None
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

            exif_data = _load_exif_data(bucket, key, content_type)

        if gps_lat is None or gps_lng is None:
            exif_lat = exif_data.get("gps_lat")
            exif_lng = exif_data.get("gps_lng")
            if _is_number(exif_lat) and _is_number(exif_lng):
                gps_lat = float(exif_lat)
                gps_lng = float(exif_lng)

        reverse_geocode = (
            _reverse_geocode(gps_lat, gps_lng) if gps_lat is not None and gps_lng is not None else None
        )

        datetime_original = _as_text(payload.get("datetime_original"))
        if datetime_original:
            item["datetime_original"] = datetime_original
        else:
            exif_datetime = _as_text(exif_data.get("datetime_original"))
            if exif_datetime:
                item["datetime_original"] = exif_datetime

        if gps_lat is not None and gps_lng is not None:
            item["gps_lat"] = gps_lat
            item["gps_lng"] = gps_lng

        camera_make = _as_text(payload.get("camera_make"))
        if camera_make:
            item["camera_make"] = camera_make
        else:
            exif_make = _as_text(exif_data.get("camera_make"))
            if exif_make:
                item["camera_make"] = exif_make

        camera_model = _as_text(payload.get("camera_model"))
        if camera_model:
            item["camera_model"] = camera_model
        else:
            exif_model = _as_text(exif_data.get("camera_model"))
            if exif_model:
                item["camera_model"] = exif_model

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
