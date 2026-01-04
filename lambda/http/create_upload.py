import json
import os
import re
import uuid
from datetime import datetime
from typing import Any, Dict

from botocore.config import Config

import boto3

_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
_S3_CONFIG = Config(signature_version="s3v4", s3={"addressing_style": "virtual"})
s3 = (
    boto3.client(
        "s3",
        region_name=_REGION,
        endpoint_url=f"https://s3.{_REGION}.amazonaws.com" if _REGION else None,
        config=_S3_CONFIG,
    )
    if _REGION
    else boto3.client("s3", config=_S3_CONFIG)
)
ddb = boto3.resource("dynamodb")

UPLOADS_BUCKET = os.environ.get("UPLOADS_BUCKET", "")
UPLOADS_TABLE = os.environ.get("UPLOADS_TABLE", "")
UPLOAD_PREFIX = os.environ.get("UPLOAD_PREFIX", "uploads")


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


def _sanitize_filename(filename: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", filename)
    return safe[:128] or "upload"


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    if not UPLOADS_BUCKET or not UPLOADS_TABLE:
        return _response(500, {"message": "Missing required configuration."})

    body: Dict[str, Any] = {}
    raw_body = event.get("body")
    if raw_body:
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError:
            return _response(400, {"message": "Invalid JSON body."})

    upload_id = str(uuid.uuid4())
    filename = _sanitize_filename(body.get("filename", "upload")) if isinstance(body.get("filename"), str) else "upload"
    content_type = body.get("content_type") if isinstance(body.get("content_type"), str) else "application/octet-stream"
    object_key = f"{UPLOAD_PREFIX}/{upload_id}/{filename}"
    created_at = datetime.utcnow().isoformat() + "Z"

    upload_url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": UPLOADS_BUCKET, "Key": object_key, "ContentType": content_type},
        ExpiresIn=900,
    )

    table = ddb.Table(UPLOADS_TABLE)
    table.put_item(
        Item={
            "upload_id": upload_id,
            "user_id": body.get("user_id", "anonymous") if isinstance(body.get("user_id"), str) else "anonymous",
            "status": "created",
            "created_at": created_at,
            "original_image_uri": f"s3://{UPLOADS_BUCKET}/{object_key}",
        }
    )

    return _response(
        200,
        {
            "upload_id": upload_id,
            "upload_url": upload_url,
            "object_key": object_key,
            "expires_in": 900,
        },
    )
