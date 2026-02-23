"""Claude API client for company research."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import anthropic
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)


@dataclass
class ResearchResponse:
    raw_text: str
    model: str
    input_tokens: int
    output_tokens: int
    duration_seconds: float


class ResearchClient:
    def __init__(self, api_key: str, model: str, timeout_seconds: int, max_retries: int = 3):
        self._client = anthropic.Anthropic(api_key=api_key)
        self.model = model
        self.timeout = timeout_seconds
        self.max_retries = max_retries

    def research_company(self, system_prompt: str, user_prompt: str) -> ResearchResponse:
        """Make one research API call. Raises on hard failure after retries."""
        start = time.monotonic()

        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=4, max=30),
            retry=retry_if_exception_type((anthropic.RateLimitError, anthropic.APIConnectionError)),
            before_sleep=lambda state: logger.warning(
                "retrying claude api call attempt=%d", state.attempt_number
            ),
        )
        def _call() -> anthropic.types.Message:
            return self._client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
                timeout=self.timeout,
            )

        message = _call()
        duration = time.monotonic() - start
        raw_text = message.content[0].text if message.content else ""

        logger.info(
            "claude_api model=%s input_tokens=%d output_tokens=%d duration_s=%.1f",
            self.model,
            message.usage.input_tokens,
            message.usage.output_tokens,
            duration,
        )

        return ResearchResponse(
            raw_text=raw_text,
            model=self.model,
            input_tokens=message.usage.input_tokens,
            output_tokens=message.usage.output_tokens,
            duration_seconds=duration,
        )
