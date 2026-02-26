"""LLM API clients for company research (Claude + MiniMax)."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import anthropic
import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


@dataclass
class ResearchResponse:
    raw_text: str
    model: str
    input_tokens: int
    output_tokens: int
    duration_seconds: float


class ResearchClient:
    """Anthropic Claude client."""

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
            before_sleep=lambda state: logger.warning("retrying claude api call attempt=%d", state.attempt_number),
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


class MiniMaxClient:
    """MiniMax client using direct HTTP calls."""

    def __init__(self, api_key: str, model: str, timeout_seconds: int, max_retries: int = 3):
        self._client = httpx.Client(
            base_url="https://api.minimax.io/v1",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        self.model = model
        self.timeout = timeout_seconds
        self.max_retries = max_retries

    def research_company(self, system_prompt: str, user_prompt: str) -> ResearchResponse:
        """Make one research API call via MiniMax. Raises on hard failure after retries."""
        start = time.monotonic()

        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=4, max=30),
            retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError)),
            before_sleep=lambda state: logger.warning("retrying minimax api call attempt=%d", state.attempt_number),
        )
        def _call() -> dict:
            response = self._client.post(
                "/chat/completions",
                json={
                    "model": self.model,
                    "max_tokens": 4096,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()

        response = _call()
        duration = time.monotonic() - start
        choices = response.get("choices") or []
        first_choice = choices[0] if choices else {}
        message = first_choice.get("message") if isinstance(first_choice, dict) else {}
        raw_text = (message or {}).get("content") or ""
        usage = response.get("usage") if isinstance(response, dict) else {}
        prompt_tokens = int((usage or {}).get("prompt_tokens") or 0)
        completion_tokens = int((usage or {}).get("completion_tokens") or 0)

        logger.info(
            "minimax_api model=%s input_tokens=%d output_tokens=%d duration_s=%.1f",
            self.model,
            prompt_tokens,
            completion_tokens,
            duration,
        )

        return ResearchResponse(
            raw_text=raw_text,
            model=self.model,
            input_tokens=prompt_tokens,
            output_tokens=completion_tokens,
            duration_seconds=duration,
        )


def create_research_client(settings) -> MiniMaxClient | ResearchClient:
    """Factory: pick the right client based on settings.llm_provider."""
    provider = getattr(settings, "llm_provider", "claude")
    if provider == "minimax":
        api_key = settings.minimax_api_key
        if not api_key:
            raise ValueError("minimax_api_key is required when llm_provider=minimax")
        return MiniMaxClient(
            api_key=api_key,
            model=settings.minimax_model,
            timeout_seconds=settings.research_timeout_seconds,
        )
    else:
        api_key = settings.claude_api_key
        if not api_key:
            raise ValueError("claude_api_key is required when llm_provider=claude")
        return ResearchClient(
            api_key=api_key,
            model=settings.claude_model,
            timeout_seconds=settings.research_timeout_seconds,
        )
