import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict

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

    path_params = event.get("pathParameters") or {}
    article_id = path_params.get("articleId")
    if not article_id:
        return _response(400, {"message": "articleId is required."})

    body: Dict[str, Any] = {}
    raw_body = event.get("body")
    if raw_body:
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError:
            return _response(400, {"message": "Invalid JSON body."})

    run_id = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat() + "Z"

    runs_table = ddb.Table(GENERATION_RUNS_TABLE)
    runs_table.put_item(
        Item={
            "run_id": run_id,
            "article_id": article_id,
            "status": "queued",
            "created_at": created_at,
            "tone": _as_string(body.get("tone"), "polite"),
            "length": _as_string(body.get("length"), "medium"),
            "language": _as_string(body.get("language"), "ja"),
            "privacy_level": _as_string(body.get("privacy_level"), "area"),
            "instruction": _as_string(body.get("instruction"), ""),
        }
    )

    articles_table = ddb.Table(ARTICLES_TABLE)
    articles_table.update_item(
        Key={"article_id": article_id},
        UpdateExpression="SET updated_at = :updated_at, #status = :status",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":updated_at": created_at,
            ":status": "draft_pending",
        },
    )

    sqs.send_message(
        QueueUrl=GENERATION_QUEUE_URL,
        MessageBody=json.dumps(
            {
                "article_id": article_id,
                "run_id": run_id,
                "tone": _as_string(body.get("tone"), "polite"),
                "length": _as_string(body.get("length"), "medium"),
                "language": _as_string(body.get("language"), "ja"),
                "privacy_level": _as_string(body.get("privacy_level"), "area"),
                "instruction": _as_string(body.get("instruction"), ""),
            }
        ),
    )

    return _response(200, {"article_id": article_id, "run_id": run_id, "status": "queued"})
