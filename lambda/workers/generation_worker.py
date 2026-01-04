import base64
import json
import mimetypes
import os
import socket
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import boto3

ddb = boto3.resource("dynamodb")
secrets = boto3.client("secretsmanager")
s3 = boto3.client("s3")

ARTICLES_TABLE = os.environ.get("ARTICLES_TABLE", "")
UPLOADS_TABLE = os.environ.get("UPLOADS_TABLE", "")
METADATA_TABLE = os.environ.get("METADATA_TABLE", "")
GENERATION_RUNS_TABLE = os.environ.get("GENERATION_RUNS_TABLE", "")
GEMINI_API_KEY_SECRET_ARN = os.environ.get("GEMINI_API_KEY_SECRET_ARN", "")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3-flash-preview")

# Optional: allow tuning without code changes
GEMINI_TEMPERATURE = float(os.environ.get("GEMINI_TEMPERATURE", "0.2"))
GEMINI_TOP_P = float(os.environ.get("GEMINI_TOP_P", "0.95"))
GEMINI_TOP_K = int(os.environ.get("GEMINI_TOP_K", "40"))
GEMINI_REQUEST_TIMEOUT = float(os.environ.get("GEMINI_REQUEST_TIMEOUT", "600"))
GEMINI_MAX_RETRIES = int(os.environ.get("GEMINI_MAX_RETRIES", "2"))

_API_KEY_CACHE: Optional[str] = None


def _build_markdown(title: str, created_at: str, privacy_level: str) -> str:
    return (
        "---\n"
        f'title: "{title}"\n'
        f"date: {created_at}\n"
        f"location: {privacy_level}\n"
        "tags: []\n"
        "---\n\n"
        f"# {title}\n\n"
        "This body is a placeholder. Replace with Gemini output after wiring the model call.\n\n"
        "## Capture info\n"
        "- captured_at: unknown\n"
        "- location: unspecified\n"
    )


def _get_api_key() -> str:
    global _API_KEY_CACHE
    if _API_KEY_CACHE is not None:
        return _API_KEY_CACHE
    if not GEMINI_API_KEY_SECRET_ARN:
        _API_KEY_CACHE = ""
        return _API_KEY_CACHE
    try:
        response = secrets.get_secret_value(SecretId=GEMINI_API_KEY_SECRET_ARN)
    except Exception as exc:
        print(f"Failed to load Gemini API key: {exc}")
        _API_KEY_CACHE = ""
        return _API_KEY_CACHE

    secret_value = response.get("SecretString")
    if not secret_value and response.get("SecretBinary"):
        try:
            secret_value = base64.b64decode(response["SecretBinary"]).decode("utf-8")
        except Exception as exc:
            print(f"Failed to decode secret binary: {exc}")
            secret_value = ""

    if not isinstance(secret_value, str) or not secret_value.strip():
        _API_KEY_CACHE = ""
        return _API_KEY_CACHE

    secret_value = secret_value.strip()
    try:
        payload = json.loads(secret_value)
        if isinstance(payload, dict):
            for key_name in ("apiKey", "key", "GEMINI_API_KEY"):
                key_value = payload.get(key_name)
                if isinstance(key_value, str) and key_value.strip():
                    _API_KEY_CACHE = key_value.strip()
                    return _API_KEY_CACHE
    except json.JSONDecodeError:
        pass

    _API_KEY_CACHE = secret_value
    return _API_KEY_CACHE


def _length_config(length: str) -> Tuple[int, str]:
    if length == "short":
        return 1024*8, "short (200-300 words, or 400-700 Japanese characters)"
    if length == "long":
        return 1024*32, "long (900-1200 words, or 1500-2200 Japanese characters)"
    return 1024*16, "medium (500-800 words, or 900-1400 Japanese characters)"


def _privacy_guideline(level: str) -> str:
    if level == "exact":
        return "Exact location is allowed."
    if level == "city":
        return "Limit location to city-level detail."
    return "Limit location to broad area or prefecture-level detail."


def _tone_label(tone: str) -> str:
    if tone == "casual":
        return "casual"
    if tone == "formal":
        return "formal"
    return "polite"


def _as_text(value: Any) -> Optional[str]:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _location_label(reverse_geocode: Any) -> Optional[str]:
    if not isinstance(reverse_geocode, dict):
        return None
    label = _as_text(reverse_geocode.get("label"))
    if label:
        return label
    parts = [reverse_geocode.get("city"), reverse_geocode.get("prefecture"), reverse_geocode.get("country")]
    parts = [part for part in (_as_text(part) for part in parts) if part]
    return ", ".join(parts) if parts else None


def _safe_get_item(table: Any, key: Dict[str, Any]) -> Dict[str, Any]:
    if table is None:
        return {}
    try:
        response = table.get_item(Key=key)
    except Exception as exc:
        print(f"Failed to load item {key}: {exc}")
        return {}
    return response.get("Item") or {}


def _extract_json(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    candidate = text.strip()
    if candidate.startswith("```"):
        candidate = candidate.strip("`")
        candidate = candidate.replace("json", "", 1).strip()
    try:
        parsed = json.loads(candidate)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass

    start = candidate.find("{")
    end = candidate.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        parsed = json.loads(candidate[start : end + 1])
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        return None


def _parse_tags_value(value: Optional[str]) -> list:
    if not value:
        return []
    raw = value.strip()
    if not raw:
        return []
    if raw.startswith("[") and raw.endswith("]"):
        try:
            parsed = json.loads(raw)
            return _normalize_tags(parsed)
        except json.JSONDecodeError:
            inner = raw[1:-1].strip()
            if not inner:
                return []
            return [part.strip().strip('"').strip("'") for part in inner.split(",") if part.strip()]
    return [part.strip().strip('"').strip("'") for part in raw.split(",") if part.strip()]


def _strip_top_heading(body: str) -> str:
    lines = body.lstrip().splitlines()
    if not lines:
        return body.strip()
    if lines[0].startswith("# "):
        lines = lines[1:]
        if lines and not lines[0].strip():
            lines = lines[1:]
    return "\n".join(lines).strip()


def _split_capture_info(body: str) -> Tuple[str, Dict[str, str]]:
    lines = body.splitlines()
    capture_index = None
    for idx, line in enumerate(lines):
        if line.strip().lower() == "## capture info":
            capture_index = idx
            break
    if capture_index is None:
        return body.strip(), {}

    capture_lines = []
    for line in lines[capture_index + 1 :]:
        stripped = line.strip()
        if stripped.startswith("## ") or stripped.startswith("# "):
            break
        capture_lines.append(line)

    body_lines = lines[:capture_index]
    capture_info: Dict[str, str] = {}
    for line in capture_lines:
        stripped = line.strip()
        if stripped.startswith("-"):
            stripped = stripped[1:].strip()
        if ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        key = key.strip().lower()
        value = value.strip()
        if key == "captured_at":
            capture_info["captured_at"] = value or "unknown"
        elif key == "location":
            capture_info["location"] = value or "unspecified"

    return "\n".join(body_lines).strip(), capture_info


def _parse_markdown_article(
    text: str, created_at: str, privacy_level: str, fallback_title: str
) -> Optional[Dict[str, Any]]:
    if not text:
        return None

    candidate = text.strip()
    if not candidate:
        return None

    lines = candidate.splitlines()
    front_matter: Dict[str, str] = {}
    body_lines = lines

    if lines and lines[0].strip() == "---":
        end_index = None
        for idx in range(1, len(lines)):
            if lines[idx].strip() == "---":
                end_index = idx
                break
        if end_index is not None:
            for line in lines[1:end_index]:
                if ":" not in line:
                    continue
                key, value = line.split(":", 1)
                front_matter[key.strip().lower()] = value.strip().strip('"')
            body_lines = lines[end_index + 1 :]

    body = "\n".join(body_lines).strip()
    if not body:
        return None

    title = _as_text(front_matter.get("title"))
    date = _as_text(front_matter.get("date"))
    location = _as_text(front_matter.get("location"))
    tags = _parse_tags_value(front_matter.get("tags"))

    if not title:
        for line in body_lines:
            if line.startswith("# "):
                title = line[2:].strip()
                break
    if not title:
        title = fallback_title

    body = _strip_top_heading(body)
    body, capture_info = _split_capture_info(body)
    if not body:
        return None

    return {
        "title": title,
        "date": date or created_at,
        "location": location or privacy_level,
        "tags": tags,
        "body_markdown": body,
        "capture_info": {
            "captured_at": capture_info.get("captured_at", "unknown"),
            "location": capture_info.get("location", "unspecified"),
        },
    }


def _article_body_length(article_json: Optional[Dict[str, Any]]) -> int:
    if not article_json:
        return 0
    body = article_json.get("body_markdown") or ""
    return len(body)


def _normalize_tags(value: Any) -> list:
    if not isinstance(value, list):
        return []
    tags = []
    for item in value:
        text = _as_text(item)
        if text:
            tags.append(text)
    return tags


def _normalize_article_json(
    data: Dict[str, Any], created_at: str, privacy_level: str, fallback_title: str
) -> Dict[str, Any]:
    title = _as_text(data.get("title")) or fallback_title
    date = _as_text(data.get("date")) or created_at
    location = _as_text(data.get("location")) or privacy_level
    tags = _normalize_tags(data.get("tags"))
    body_markdown = _as_text(data.get("body_markdown")) or ""

    capture = data.get("capture_info") if isinstance(data.get("capture_info"), dict) else {}
    captured_at = _as_text(capture.get("captured_at")) or "unknown"
    capture_location = _as_text(capture.get("location")) or "unspecified"

    return {
        "title": title,
        "date": date,
        "location": location,
        "tags": tags,
        "body_markdown": body_markdown,
        "capture_info": {"captured_at": captured_at, "location": capture_location},
    }


def _coerce_response_to_article_json(
    response_text: str,
    created_at: str,
    privacy_level: str,
    fallback_title: str,
) -> Tuple[Optional[Dict[str, Any]], str]:
    candidate = _extract_json(response_text)
    source = "json"

    if not candidate or not _as_text(candidate.get("body_markdown")):
        candidate = _parse_markdown_article(response_text, created_at, privacy_level, fallback_title)
        source = "markdown"

    if not candidate:
        return None, "Gemini output was not valid JSON; fallback markdown used."

    normalized = _normalize_article_json(candidate, created_at, privacy_level, fallback_title)
    warning = ""
    if source == "markdown":
        warning = "Gemini output was not valid JSON; parsed as markdown."

    return normalized, warning


def _build_markdown_from_json(article_json: Dict[str, Any]) -> str:
    title = article_json.get("title") or "Untitled"
    date = article_json.get("date") or datetime.utcnow().isoformat() + "Z"
    location = article_json.get("location") or "area"
    tags = article_json.get("tags") or []
    body = article_json.get("body_markdown") or ""
    capture = article_json.get("capture_info") or {}
    captured_at = capture.get("captured_at") or "unknown"
    capture_location = capture.get("location") or "unspecified"

    tags_yaml = json.dumps(tags, ensure_ascii=False) if tags else "[]"
    body = body.strip()

    return (
        "---\n"
        f'title: "{title}"\n'
        f"date: {date}\n"
        f"location: {location}\n"
        f"tags: {tags_yaml}\n"
        "---\n\n"
        f"# {title}\n\n"
        f"{body}\n\n"
        "## Capture info\n"
        f"- captured_at: {captured_at}\n"
        f"- location: {capture_location}\n"
    )


def _resolve_upload_ids(payload: Dict[str, Any], articles_table: Any, article_id: str) -> list:
    upload_ids = payload.get("upload_ids") if isinstance(payload.get("upload_ids"), list) else []
    upload_ids = [uid.strip() for uid in upload_ids if isinstance(uid, str) and uid.strip()]
    if upload_ids:
        return upload_ids
    article_item = _safe_get_item(articles_table, {"article_id": article_id})
    derived = article_item.get("derived_from_upload_ids")
    if not isinstance(derived, list):
        return []
    return [uid.strip() for uid in derived if isinstance(uid, str) and uid.strip()]


def _format_photo_context(
    upload_ids: list, uploads_table: Any, metadata_table: Any
) -> str:
    if not upload_ids:
        return ""
    lines = []
    for index, upload_id in enumerate(upload_ids, start=1):
        upload_item = _safe_get_item(uploads_table, {"upload_id": upload_id})
        metadata_item = _safe_get_item(metadata_table, {"upload_id": upload_id})

        parts = [f"id={upload_id}"]
        file_ref = _as_text(metadata_item.get("object_key")) or _as_text(upload_item.get("original_image_uri"))
        if file_ref:
            parts.append(f"file={file_ref}")
        captured_at = _as_text(metadata_item.get("datetime_original"))
        if captured_at:
            parts.append(f"captured_at={captured_at}")
        uploaded_at = _as_text(upload_item.get("created_at"))
        if uploaded_at:
            parts.append(f"uploaded_at={uploaded_at}")
        camera_make = _as_text(metadata_item.get("camera_make"))
        camera_model = _as_text(metadata_item.get("camera_model"))
        if camera_make or camera_model:
            camera = " ".join(part for part in (camera_make, camera_model) if part)
            parts.append(f"camera={camera}")
        location = _location_label(metadata_item.get("reverse_geocode"))
        if location:
            parts.append(f"location={location}")
        content_type = _as_text(metadata_item.get("content_type"))
        if content_type:
            parts.append(f"type={content_type}")
        content_length = metadata_item.get("content_length")
        if isinstance(content_length, (int, float)) and content_length > 0:
            size_kb = int(content_length / 1024)
            parts.append(f"size_kb={size_kb}")

        lines.append(f"{index}. " + "; ".join(parts))
    return "\n".join(lines)


def _parse_s3_uri(uri: str) -> Optional[Tuple[str, str]]:
    if not uri:
        return None
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        return None
    return parsed.netloc, parsed.path.lstrip("/")


def _resolve_s3_location(upload_item: Dict[str, Any], metadata_item: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    bucket = _as_text(metadata_item.get("object_bucket"))
    key = _as_text(metadata_item.get("object_key"))
    if bucket and key:
        return bucket, key

    s3_uri = _as_text(metadata_item.get("s3_uri")) or _as_text(upload_item.get("original_image_uri"))
    if s3_uri:
        parsed = _parse_s3_uri(s3_uri)
        if parsed:
            return parsed

    return None, None


def _load_image_parts(upload_ids: list, uploads_table: Any, metadata_table: Any) -> list:
    if not upload_ids:
        return []
    parts = []
    for upload_id in upload_ids:
        upload_item = _safe_get_item(uploads_table, {"upload_id": upload_id}) if uploads_table else {}
        metadata_item = _safe_get_item(metadata_table, {"upload_id": upload_id}) if metadata_table else {}
        bucket, key = _resolve_s3_location(upload_item, metadata_item)
        if not bucket or not key:
            print(f"Missing S3 location for upload {upload_id}")
            continue
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            body = response.get("Body")
            if body is None:
                print(f"Missing image body for {bucket}/{key}")
                continue
            data = body.read()
        except Exception as exc:
            print(f"Failed to load image {bucket}/{key}: {exc}")
            continue
        if not data:
            print(f"Empty image data for {bucket}/{key}")
            continue

        content_type = response.get("ContentType")
        if not isinstance(content_type, str) or not content_type.strip():
            guessed = mimetypes.guess_type(key)[0]
            content_type = guessed or "image/jpeg"

        parts.append(
            {"inlineData": {"mimeType": content_type, "data": base64.b64encode(data).decode("ascii")}}
        )
    return parts


def _min_chars_for_length(length: str) -> int:
    if length == "short":
        return 300
    if length == "long":
        return 1200
    return 600


def _build_prompt(
    payload: Dict[str, Any],
    created_at: str,
    upload_ids: list,
    photo_context: str,
    min_chars: int,
    retry: bool = False,
) -> Tuple[str, int]:
    language = payload.get("language") if isinstance(payload.get("language"), str) else "ja"
    tone = _tone_label(payload.get("tone") if isinstance(payload.get("tone"), str) else "polite")
    length = payload.get("length") if isinstance(payload.get("length"), str) else "medium"
    privacy_level = payload.get("privacy_level") if isinstance(payload.get("privacy_level"), str) else "area"
    instruction = payload.get("instruction") if isinstance(payload.get("instruction"), str) else ""
    max_tokens, length_hint = _length_config(length)
    language_hint = "Write in Japanese." if language == "ja" else "Write in English."

    prompt = (
        "You are drafting a blog post based on photo uploads.\n"
        f"Photo count: {len(upload_ids)}.\n"
        "Photos are provided as image inputs.\n"
        f"{language_hint}\n"
        f"Tone: {tone}.\n"
        f"Length: {length_hint}.\n"
        f"Privacy: {_privacy_guideline(privacy_level)}\n"
        "Do not invent specific camera, time, or location details. If unknown, use 'unknown' or 'unspecified'.\n"
        "Return JSON only, without code fences.\n"
        f"body_markdown must be at least {min_chars} characters.\n"
        "JSON schema:\n"
        "{\n"
        '  "title": "string",\n'
        '  "date": "string",\n'
        '  "location": "string",\n'
        '  "tags": ["string"],\n'
        '  "body_markdown": "string (no front matter, no top-level title)",\n'
        '  "capture_info": {"captured_at": "string", "location": "string"}\n'
        "}\n"
        f"User instruction: {instruction or 'none'}\n"
    )
    if retry:
        prompt += (
            "\nThe previous output was too short or incomplete. "
            f"Return JSON with body_markdown at least {min_chars} characters.\n"
        )
    if photo_context:
        prompt += f"\nPhoto details:\n{photo_context}\n"
    return prompt, max_tokens


def _build_expand_prompt(payload: Dict[str, Any], article_json: Dict[str, Any], min_chars: int) -> Tuple[str, int]:
    language = payload.get("language") if isinstance(payload.get("language"), str) else "ja"
    tone = _tone_label(payload.get("tone") if isinstance(payload.get("tone"), str) else "polite")
    privacy_level = payload.get("privacy_level") if isinstance(payload.get("privacy_level"), str) else "area"
    instruction = payload.get("instruction") if isinstance(payload.get("instruction"), str) else ""
    # NOTE: original code had a bug here; fixed to keep max_tokens as int.
    max_tokens, _ = _length_config(payload.get("length") if isinstance(payload.get("length"), str) else "medium")
    language_hint = "Write in Japanese." if language == "ja" else "Write in English."
    draft = json.dumps(article_json, ensure_ascii=False)

    prompt = (
        "You are improving an existing blog draft.\n"
        "Photos are provided as image inputs.\n"
        f"{language_hint}\n"
        f"Tone: {tone}.\n"
        f"Privacy: {_privacy_guideline(privacy_level)}\n"
        f"Expand body_markdown to at least {min_chars} characters.\n"
        "Keep title/date/location/tags and capture_info consistent unless you need to add detail.\n"
        "Return JSON only, without code fences, using the same schema:\n"
        "{\n"
        '  "title": "string",\n'
        '  "date": "string",\n'
        '  "location": "string",\n'
        '  "tags": ["string"],\n'
        '  "body_markdown": "string (no front matter, no top-level title)",\n'
        '  "capture_info": {"captured_at": "string", "location": "string"}\n'
        "}\n"
        f"User instruction: {instruction or 'none'}\n"
        f"Current draft JSON:\n{draft}\n"
    )
    return prompt, max_tokens


def _build_response_json_schema() -> Dict[str, Any]:
    """
    JSON Schema for Gemini structured output.
    IMPORTANT:
      - Use generationConfig.responseJsonSchema (NOT responseSchema)
      - additionalProperties is supported in responseJsonSchema subset
    """
    return {
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "date": {"type": "string"},
            "location": {"type": "string"},
            "tags": {"type": "array", "items": {"type": "string"}},
            "body_markdown": {"type": "string"},
            "capture_info": {
                "type": "object",
                "properties": {
                    "captured_at": {"type": "string"},
                    "location": {"type": "string"},
                },
                "required": ["captured_at", "location"],
                "additionalProperties": False,
            },
        },
        "required": ["title", "date", "location", "tags", "body_markdown", "capture_info"],
        "additionalProperties": False,
    }


def _log_gemini_payload(payload: Dict[str, Any]) -> None:
    try:
        print("Gemini response payload: " + json.dumps(payload, ensure_ascii=False))
    except (TypeError, ValueError) as exc:
        print(f"Failed to serialize Gemini response payload: {exc}")


def _call_gemini(
    api_key: str, prompt: str, max_output_tokens: int, image_parts: Optional[list] = None
) -> Tuple[str, str]:
    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
        f"?key={api_key}"
    )

    # A対応（修正版）: responseSchema ではなく responseJsonSchema を使う
    response_json_schema = _build_response_json_schema()

    parts = [{"text": prompt}]
    if image_parts:
        parts.extend(image_parts)

    body = {
        "contents": [{"role": "user", "parts": parts}],
        "generationConfig": {
            "temperature": GEMINI_TEMPERATURE,  # 例: 0.2
            "maxOutputTokens": int(max_output_tokens),
            "topP": GEMINI_TOP_P,
            "topK": GEMINI_TOP_K,
            "responseMimeType": "application/json",
            # IMPORTANT: Use responseJsonSchema (omit responseSchema)
            "responseJsonSchema": response_json_schema,
        },
    }

    request = urllib.request.Request(
        endpoint,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    last_exc: Optional[Exception] = None
    for attempt in range(GEMINI_MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=GEMINI_REQUEST_TIMEOUT) as response:
                raw = response.read()
            last_exc = None
            break
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Gemini API error: {exc.code} {detail}") from exc
        except (socket.timeout, TimeoutError) as exc:
            last_exc = exc
            if attempt >= GEMINI_MAX_RETRIES:
                break
            backoff = min(2 ** attempt, 8)
            print(f"Gemini request timed out; retrying in {backoff}s (attempt {attempt + 1}).")
            time.sleep(backoff)
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, socket.timeout):
                last_exc = exc
                if attempt >= GEMINI_MAX_RETRIES:
                    break
                backoff = min(2 ** attempt, 8)
                print(f"Gemini request timed out; retrying in {backoff}s (attempt {attempt + 1}).")
                time.sleep(backoff)
            else:
                raise RuntimeError(f"Gemini API request failed: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Gemini API request failed: {exc}") from exc

    if last_exc is not None:
        raise RuntimeError(f"Gemini API request failed: {last_exc}") from last_exc

    payload = json.loads(raw.decode("utf-8"))
    _log_gemini_payload(payload)
    candidates = payload.get("candidates") or []
    if not candidates:
        return "", GEMINI_MODEL
    content = candidates[0].get("content") or {}
    parts = content.get("parts") or []
    text = "".join(part.get("text", "") for part in parts if isinstance(part, dict))
    return text.strip(), GEMINI_MODEL


def handler(event: Dict[str, Any], _context: Any) -> None:
    if not ARTICLES_TABLE or not GENERATION_RUNS_TABLE:
        return

    articles_table = ddb.Table(ARTICLES_TABLE)
    runs_table = ddb.Table(GENERATION_RUNS_TABLE)
    uploads_table = ddb.Table(UPLOADS_TABLE) if UPLOADS_TABLE else None
    metadata_table = ddb.Table(METADATA_TABLE) if METADATA_TABLE else None

    for record in event.get("Records", []):
        try:
            payload = json.loads(record.get("body") or "{}")
        except json.JSONDecodeError:
            continue

        article_id = payload.get("article_id")
        run_id = payload.get("run_id")
        if not isinstance(article_id, str) or not isinstance(run_id, str):
            continue

        created_at = datetime.utcnow().isoformat() + "Z"
        privacy_level = payload.get("privacy_level") if isinstance(payload.get("privacy_level"), str) else "area"
        title = f"Auto draft {article_id[:8]}"

        api_key = _get_api_key()
        markdown = ""
        model_used = GEMINI_MODEL
        error_message = ""

        upload_ids = _resolve_upload_ids(payload, articles_table, article_id)
        photo_context = _format_photo_context(upload_ids, uploads_table, metadata_table)
        image_parts = _load_image_parts(upload_ids, uploads_table, metadata_table)

        length = payload.get("length") if isinstance(payload.get("length"), str) else "medium"
        min_chars = _min_chars_for_length(length)

        article_json: Optional[Dict[str, Any]] = None

        if api_key:
            try:
                prompt, max_tokens = _build_prompt(payload, created_at, upload_ids, photo_context, min_chars)
                response_text, model_used = _call_gemini(api_key, prompt, max_tokens, image_parts)
                article_json, warning = _coerce_response_to_article_json(
                    response_text, created_at, privacy_level, title
                )
                error_message = warning

                if _article_body_length(article_json) < min_chars:
                    retry_prompt, retry_max_tokens = _build_prompt(
                        payload, created_at, upload_ids, photo_context, min_chars, retry=True
                    )
                    retry_max_tokens = max(retry_max_tokens, 1200)
                    retry_text, model_used = _call_gemini(
                        api_key, retry_prompt, retry_max_tokens, image_parts
                    )
                    retry_article, retry_warning = _coerce_response_to_article_json(
                        retry_text, created_at, privacy_level, title
                    )
                    if retry_article:
                        article_json = retry_article
                        error_message = retry_warning

                if article_json and _article_body_length(article_json) < min_chars:
                    expand_prompt, expand_max_tokens = _build_expand_prompt(payload, article_json, min_chars)
                    expand_max_tokens = max(expand_max_tokens, 1200)
                    expand_text, model_used = _call_gemini(
                        api_key, expand_prompt, expand_max_tokens, image_parts
                    )
                    expanded_article, expand_warning = _coerce_response_to_article_json(
                        expand_text, created_at, privacy_level, title
                    )
                    if expanded_article and _article_body_length(expanded_article) >= _article_body_length(article_json):
                        article_json = expanded_article
                        error_message = expand_warning

                if article_json and _article_body_length(article_json) < min_chars:
                    suffix = "Output shorter than requested."
                    error_message = f"{error_message} {suffix}".strip()
                elif article_json and _article_body_length(article_json) >= min_chars:
                    error_message = ""
            except Exception as exc:
                error_message = str(exc)
                print(error_message)
        else:
            error_message = "Gemini API key is missing."

        if article_json:
            title = article_json.get("title") or title
            markdown = _build_markdown_from_json(article_json)
        else:
            markdown = ""

        if not markdown:
            markdown = _build_markdown(title, created_at, privacy_level)
            if not error_message:
                error_message = "Gemini output was empty; fallback markdown used."

        articles_table.update_item(
            Key={"article_id": article_id},
            UpdateExpression=(
                "SET #status = :status, updated_at = :updated_at, title = :title, "
                "body_markdown = :body, body_json = :body_json"
            ),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "draft",
                ":updated_at": created_at,
                ":title": title,
                ":body": markdown,
                ":body_json": article_json or {"status": "fallback"},
            },
        )

        runs_table.update_item(
            Key={"run_id": run_id},
            UpdateExpression=(
                "SET #status = :status, completed_at = :completed_at, model = :model, error_message = :error"
            ),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "completed",
                ":completed_at": created_at,
                ":model": model_used,
                ":error": error_message,
            },
        )
