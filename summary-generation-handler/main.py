import logging
import os
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
CONSUMER_GROUP = "summary_handlers"

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _update_meta(meta_key: str, mapping: Dict[str, Any]) -> None:
    client = get_redis_client()
    mapping["updated_at"] = _now_iso()
    client.hset(meta_key, mapping=mapping)


def _get_consumer_name() -> str:
    """Generate a unique consumer name for this instance."""
    pid = os.getpid()
    return f"text-summarizer-handler-{pid}"


def _ensure_consumer_group(client) -> None:
    """
    Verify the consumer group exists (fallback - should be created by backend on startup).
    This is a lightweight check that only creates if missing.
    """
    try:
        client.xgroup_create(
            name=STREAM_TEXT_READY,
            groupname=CONSUMER_GROUP,
            id="0",
            mkstream=True  # Create stream if it doesn't exist
        )
        logging.info("Created consumer group '%s' for stream '%s'", CONSUMER_GROUP, STREAM_TEXT_READY)
    except Exception as exc:
        error_msg = str(exc).lower()
        if "busygroup" in error_msg or "already exists" in error_msg:
            logging.info("Consumer group '%s' already exists", CONSUMER_GROUP)
        else:
            logging.warning("Unexpected error creating consumer group: %s", exc)


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


def _process_message(fields: Dict[Any, Any], client) -> bool:
    """
    Process a single message from the stream.
    Returns True if processing was successful and message should be acknowledged.
    """
    
    file_id = fields.get("file_id")
    meta_key = fields.get("meta_key") or META_KEY_TEMPLATE.format(file_id=file_id)
    extraction_mode = fields.get("extraction_mode")

    logging.info("Generating summary for the file: %s", file_id)
    
    if not file_id:
        logging.warning("Received text_ready event without file_id")
        return True

    text = client.hget(meta_key, "text") or ""
    
    if not text.strip():
        logging.error("No text available for summary for file_id=%s", file_id)
        _update_meta(
            meta_key,
            {"status": "error", "error": "missing_text_for_summary"},
        )
        return True

    try:
        summary = summarize_text(text, extraction_mode)
    except Exception as exc:
        logging.exception("Failed to summarize text for %s: %s", file_id, exc)
        _update_meta(
            meta_key,
            {"status": "error", "error": "summary_failed"},
        )
        
        return False

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
    
    return True

def consume_text_ready_stream() -> None:
    client = get_redis_client()
    consumer_name = _get_consumer_name()
    
    _ensure_consumer_group(client)
    
    logging.info("Starting consumer '%s' in group '%s'", consumer_name, CONSUMER_GROUP)
    
    while True:
        try:
            entries = client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=consumer_name,
                streams={STREAM_TEXT_READY: ">"},
                count=100,
                block=5000
            )
            
            if not entries:
                continue

            for _, messages in entries:
                for message_id, raw_fields in messages:
                    success = _process_message(raw_fields, client)
                    if success:
                        logging.info("Acknowledged message: %s", message_id)
                        client.xack(STREAM_TEXT_READY, CONSUMER_GROUP, message_id)
                    
        except Exception as exc:
            logging.exception("Error consuming text_ready stream: %s", exc)
            time.sleep(1)


if __name__ == "__main__":
    consume_text_ready_stream()

