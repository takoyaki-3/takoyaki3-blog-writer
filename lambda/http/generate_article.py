import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List

import boto3

ddb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

ARTICLES_TABLE = os.environ.get("ARTICLES_TABLE", "")
GENERATION_RUNS_TABLE = os.environ.get("GENERATION_RUNS_TABLE", "")
GENERATION_QUEUE_URL = os.environ.get("GENERATION_QUEUE_URL", "")


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


def _as_string(value: Any, default: str) -> str:
    return value if isinstance(value, str) else default


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    if not ARTICLES_TABLE or not GENERATION_RUNS_TABLE or not GENERATION_QUEUE_URL:
        return _response(500, {"message": "Missing required configuration."})

    body: Dict[str, Any] = {}
    raw_body = event.get("body")
    if raw_body:
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError:
            return _response(400, {"message": "Invalid JSON body."})

    upload_ids: List[str] = body.get("upload_ids") if isinstance(body.get("upload_ids"), list) else []
    if not upload_ids:
        return _response(400, {"message": "upload_ids is required."})

    article_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat() + "Z"

    articles_table = ddb.Table(ARTICLES_TABLE)
    articles_table.put_item(
        Item={
            "article_id": article_id,
            "user_id": _as_string(body.get("user_id"), "anonymous"),
            "status": "draft_pending",
            "visibility": "draft",
            "created_at": created_at,
            "updated_at": created_at,
            "derived_from_upload_ids": upload_ids,
            "location_display_level": _as_string(body.get("privacy_level"), "area"),
        }
    )

    runs_table = ddb.Table(GENERATION_RUNS_TABLE)
    runs_table.put_item(
        Item={
            "run_id": run_id,
            "article_id": article_id,
            "upload_ids": upload_ids,
            "status": "queued",
            "created_at": created_at,
            "tone": _as_string(body.get("tone"), "polite"),
            "length": _as_string(body.get("length"), "medium"),
            "language": _as_string(body.get("language"), "ja"),
            "privacy_level": _as_string(body.get("privacy_level"), "area"),
            "instruction": _as_string(body.get("instruction"), ""),
        }
    )

    sqs.send_message(
        QueueUrl=GENERATION_QUEUE_URL,
        MessageBody=json.dumps(
            {
                "article_id": article_id,
                "run_id": run_id,
                "upload_ids": upload_ids,
                "tone": _as_string(body.get("tone"), "polite"),
                "length": _as_string(body.get("length"), "medium"),
                "language": _as_string(body.get("language"), "ja"),
                "privacy_level": _as_string(body.get("privacy_level"), "area"),
                "instruction": _as_string(body.get("instruction"), ""),
            }
        ),
    )

    return _response(200, {"article_id": article_id, "run_id": run_id, "status": "queued"})
