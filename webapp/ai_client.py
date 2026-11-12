"""Shared Claude client: one structured-JSON request, checked before it is trusted."""
import json
import os

import anthropic

DEFAULT_AI_MODEL = "claude-opus-5-5"
FALLBACK_BETA = "server-side-fallback-2026-07-01"


def ai_model():
    return os.getenv("ANTHROPIC_MODEL", "").strip() or DEFAULT_AI_MODEL


def has_ai_key():
    return bool(os.getenv("ANTHROPIC_API_KEY", "").strip())


class AIResponseError(ValueError):
    """The model did not return a complete, usable answer."""


def structured_json(system, content, schema, *, effort="medium", max_tokens=16000, timeout=600):
    """Send one request whose reply must match `schema`; return the parsed object.

    Streams so long extractions are not cut off by HTTP timeouts. On a policy
    decline the API retries the same request on its default fallback model.
    """
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""), timeout=timeout)
    try:
        with client.beta.messages.stream(
            model=ai_model(),
            max_tokens=max_tokens,
            betas=[FALLBACK_BETA],
            fallbacks="default",
            system=system,
            messages=[{"role": "user", "content": content}],
            output_config={"effort": effort,
                           "format": {"type": "json_schema", "schema": schema}},
        ) as stream:
            message = stream.get_final_message()
    finally:
        client.close()

    if message.stop_reason == "refusal":
        raise AIResponseError("The AI request was declined")
    if message.stop_reason == "max_tokens":
        raise AIResponseError("The AI response exceeded its token budget")
    if message.stop_reason != "end_turn":
        raise AIResponseError("The AI response was not completed")
    text = "".join(block.text for block in message.content if block.type == "text")
    return json.loads(text)
