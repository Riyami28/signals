"""LLM API clients for company research (Claude + MiniMax)."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import anthropic
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
    """MiniMax client using OpenAI-compatible API."""

    def __init__(self, api_key: str, model: str, timeout_seconds: int, max_retries: int = 3):
        import openai

        self._client = openai.OpenAI(
            api_key=api_key,
            base_url="https://api.minimax.io/v1",
        )
        self.model = model
        self.timeout = timeout_seconds
        self.max_retries = max_retries
        self._openai = openai

    def research_company(self, system_prompt: str, user_prompt: str) -> ResearchResponse:
        """Make one research API call via MiniMax. Raises on hard failure after retries."""
        start = time.monotonic()
        openai = self._openai

        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=4, max=30),
            retry=retry_if_exception_type((openai.RateLimitError, openai.APIConnectionError)),
            before_sleep=lambda state: logger.warning("retrying minimax api call attempt=%d", state.attempt_number),
        )
        def _call():
            return self._client.chat.completions.create(
                model=self.model,
                max_tokens=4096,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                timeout=self.timeout,
            )

        response = _call()
        duration = time.monotonic() - start
        raw_text = response.choices[0].message.content or "" if response.choices else ""
        usage = response.usage

        logger.info(
            "minimax_api model=%s input_tokens=%d output_tokens=%d duration_s=%.1f",
            self.model,
            usage.prompt_tokens if usage else 0,
            usage.completion_tokens if usage else 0,
            duration,
        )

        return ResearchResponse(
            raw_text=raw_text,
            model=self.model,
            input_tokens=usage.prompt_tokens if usage else 0,
            output_tokens=usage.completion_tokens if usage else 0,
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
