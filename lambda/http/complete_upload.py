import json
import os
from datetime import datetime
from typing import Any, Dict

import boto3

ddb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

UPLOADS_BUCKET = os.environ.get("UPLOADS_BUCKET", "")
UPLOADS_TABLE = os.environ.get("UPLOADS_TABLE", "")
UPLOAD_PREFIX = os.environ.get("UPLOAD_PREFIX", "uploads")
EXIF_QUEUE_URL = os.environ.get("EXIF_QUEUE_URL", "")


def _response(status_code: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
        },
        "body": json.dumps(payload),
    }


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    if not UPLOADS_TABLE or not EXIF_QUEUE_URL or not UPLOADS_BUCKET:
        return _response(500, {"message": "Missing required configuration."})

    path_params = event.get("pathParameters") or {}
    upload_id = path_params.get("uploadId")
    if not upload_id:
        return _response(400, {"message": "uploadId is required."})

    body: Dict[str, Any] = {}
    raw_body = event.get("body")
    if raw_body:
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError:
            return _response(400, {"message": "Invalid JSON body."})

    object_key = body.get("object_key") if isinstance(body.get("object_key"), str) else f"{UPLOAD_PREFIX}/{upload_id}"
    updated_at = datetime.utcnow().isoformat() + "Z"

    table = ddb.Table(UPLOADS_TABLE)
    table.update_item(
        Key={"upload_id": upload_id},
        UpdateExpression=(
            "SET #status = :status, updated_at = :updated_at, "
            "original_image_uri = if_not_exists(original_image_uri, :uri)"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": "uploaded",
            ":updated_at": updated_at,
            ":uri": f"s3://{UPLOADS_BUCKET}/{object_key}",
        },
    )

    sqs.send_message(
        QueueUrl=EXIF_QUEUE_URL,
        MessageBody=json.dumps({"upload_id": upload_id, "bucket": UPLOADS_BUCKET, "key": object_key}),
    )

    return _response(200, {"upload_id": upload_id, "status": "queued"})
