import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict

from google import genai
from google.genai import types
from redis_client import get_redis_client

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s     %(message)s',
    stream=sys.stdout
)

STREAM_TEXT_READY = "pdf:text_ready"
STREAM_SUMMARY_READY = "pdf:summary_ready"
META_KEY_TEMPLATE = "pdf:meta:{file_id}"

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _decode_field(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode()
    return str(value)


def _decode_stream_fields(fields: Dict[Any, Any]) -> Dict[str, str]:
    return {_decode_field(k): _decode_field(v) for k, v in fields.items()}


def _update_meta(meta_key: str, mapping: Dict[str, Any]) -> None:
    client = get_redis_client()
    mapping["updated_at"] = _now_iso()
    client.hset(meta_key, mapping=mapping)


def summarize_text(text: str, extraction_mode: str) -> str:
    text = text.strip()
    if not text:
        return ""

    gemini_client = genai.Client()

    prompt = (
        "Summarize the provided document text in 3-5 concise sentences. "
        "Focus on the main ideas, key facts, and outcomes. "
        "Do not include metadata, instructions, or apologies."
    )
    
    if extraction_mode == "markdown":
        prompt += (
            "The input text format is Markdown. "
            "Please return your summary also formatted as Markdown."
        )

    text = types.Part.from_text(text=f"{prompt}\n\n{text}")

    response = gemini_client.models.generate_content(
        model='gemini-2.5-flash-lite',
        contents=types.Part.from_text(text=f"{prompt}\n\n{text}"),
        config=types.GenerateContentConfig(
            temperature=0,
            top_p=0.95,
            top_k=20,
        ),
    )

    if not response or not getattr(response, "text", ""):
        raise RuntimeError("Received empty response from Gemini")

    return response.text.strip()


def consume_text_ready_stream() -> None:
    client = get_redis_client()
    last_id = "$"
    while True:
        try:
            entries = client.xread({STREAM_TEXT_READY: last_id}, block=5000, count=1)
            if not entries:
                continue

            for _, messages in entries:
                
                for message_id, raw_fields in messages:
                    last_id = message_id
                    fields = _decode_stream_fields(raw_fields)
                    file_id = fields.get("file_id")
                    meta_key = fields.get("meta_key") or META_KEY_TEMPLATE.format(file_id=file_id)
                    extraction_mode = fields.get("extraction_mode")

                    logging.info("Genearting summary for the file: %s", file_id)
                    
                    if not file_id:
                        logging.warning("Received text_ready event without file_id")
                        continue

                    text_bytes = client.hget(meta_key, "text")
                    text = text_bytes.decode() if text_bytes else ""
                    if not text.strip():
                        logging.error("No text available for summary for file_id=%s", file_id)
                        _update_meta(
                            meta_key,
                            {"status": "error", "error": "missing_text_for_summary"},
                        )
                        continue

                    try:
                        summary = summarize_text(text, extraction_mode)
                    except Exception as exc:  # pragma: no cover - defensive
                        logging.exception("Failed to summarize text for %s: %s", file_id, exc)
                        _update_meta(
                            meta_key,
                            {"status": "error", "error": "summary_failed"},
                        )
                        continue

                    _update_meta(
                        meta_key,
                        {"status": "summary_ready", "summary": summary},
                    )

                    client.xadd(
                        STREAM_SUMMARY_READY,
                        {
                            "file_id": file_id,
                            "meta_key": meta_key,
                        },
                    )
        except Exception as exc:  # pragma: no cover - defensive
            logging.exception("Error consuming text_ready stream: %s", exc)
            time.sleep(1)


if __name__ == "__main__":
    logging.info("Starting summary worker for text_ready stream")
    consume_text_ready_stream()

