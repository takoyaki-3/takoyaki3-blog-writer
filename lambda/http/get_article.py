import json
import os
from decimal import Decimal
from typing import Any, Dict

import boto3

ddb = boto3.resource("dynamodb")

ARTICLES_TABLE = os.environ.get("ARTICLES_TABLE", "")


def _response(status_code: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "OPTIONS,GET",
        },
        "body": json.dumps(payload, default=_json_default),
    }


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    return str(value)


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    if not ARTICLES_TABLE:
        return _response(500, {"message": "Missing required configuration."})

    path_params = event.get("pathParameters") or {}
    article_id = path_params.get("articleId")
    if not article_id:
        return _response(400, {"message": "articleId is required."})

    table = ddb.Table(ARTICLES_TABLE)
    response = table.get_item(Key={"article_id": article_id})
    item = response.get("Item")
    if not item:
        return _response(404, {"message": "Article not found."})

    return _response(200, {"article": item})
