"""Discovery provider registry pattern.

Allows easy swapping between different discovery sources (Apollo, Serper, etc.)
without changing the main discovery logic.

Usage:
    registry = DiscoveryRegistry(settings)
    provider = registry.get_provider()  # Auto-selects best available
    contacts = provider.discover(domain="example.com", company_name="Example Inc")
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

from src.integrations.apollo import (
    BROAD_DEPARTMENTS,
    BROAD_SENIORITIES,
    ApolloClient,
    ApolloContact,
)
from src.integrations.serp_discover import SerpDiscoverer

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredContact:
    """Standardized contact from any discovery source."""

    first_name: str
    last_name: str
    title: str
    email: str
    linkedin_url: str
    management_level: str = "IC"
    year_joined: int | None = None
    department: str = ""
    enrichment_source: str = "unknown"
    is_stale: bool = False
    confidence_score: float = 1.0


@dataclass
class DiscoveryResult:
    """Result from discovery provider."""

    contacts: list[DiscoveredContact]
    source: str
    message: str = ""
    total_found: int = 0
    credits_used: int = 0


class DiscoveryProvider(ABC):
    """Abstract base class for discovery providers."""

    @abstractmethod
    def is_configured(self) -> bool:
        """Check if provider is properly configured."""
        pass

    @abstractmethod
    def discover(
        self,
        domain: str,
        company_name: str,
        limit: int = 50,
    ) -> DiscoveryResult:
        """Discover contacts at a company.

        Args:
            domain: Company domain (e.g., "example.com")
            company_name: Company name (e.g., "Example Inc")
            limit: Max contacts to return

        Returns:
            DiscoveryResult with discovered contacts
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name for logging/display."""
        pass

    @property
    @abstractmethod
    def priority(self) -> int:
        """Priority for auto-selection (higher = preferred)."""
        pass


class ApolloDiscoveryProvider(DiscoveryProvider):
    """Apollo.io discovery provider."""

    def __init__(self, api_key: str, rate_limit: int = 50):
        self._api_key = api_key
        self._rate_limit = rate_limit
        self._client = ApolloClient(api_key=api_key, rate_limit=rate_limit)

    def is_configured(self) -> bool:
        """Apollo is configured if API key is provided."""
        return bool(self._api_key)

    @property
    def name(self) -> str:
        return "Apollo"

    @property
    def priority(self) -> int:
        return 100  # Highest priority - best data quality

    def discover(
        self,
        domain: str,
        company_name: str,
        limit: int = 50,
    ) -> DiscoveryResult:
        """Discover using Apollo's broad search."""
        try:
            result = self._client.search_people_broad(
                domain=domain,
                departments=BROAD_DEPARTMENTS,
                seniority_levels=BROAD_SENIORITIES,
                limit=limit,
            )

            contacts = [
                DiscoveredContact(
                    first_name=c.first_name,
                    last_name=c.last_name,
                    title=c.title,
                    email=c.email,
                    linkedin_url=c.linkedin_url,
                    management_level=c.management_level,
                    year_joined=c.year_joined,
                    enrichment_source="apollo",
                    confidence_score=0.95,
                )
                for c in result.contacts
                if c.first_name and c.last_name
            ]

            return DiscoveryResult(
                contacts=contacts,
                source="apollo",
                total_found=result.total_found,
                credits_used=result.api_credits_used,
                message=f"Found {len(contacts)} contacts via Apollo",
            )
        except Exception as e:
            logger.warning(f"Apollo discovery failed: {e}")
            return DiscoveryResult(
                contacts=[],
                source="apollo",
                message=f"Apollo discovery failed: {e}",
            )


class SerperDiscoveryProvider(DiscoveryProvider):
    """Serper.dev (Google Search LinkedIn) discovery provider."""

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._discoverer = SerpDiscoverer(api_key=api_key)

    def is_configured(self) -> bool:
        """Serper is configured if API key is provided."""
        return bool(self._api_key)

    @property
    def name(self) -> str:
        return "Serper"

    @property
    def priority(self) -> int:
        return 50  # Medium priority - good fallback

    def discover(
        self,
        domain: str,
        company_name: str,
        limit: int = 50,
    ) -> DiscoveryResult:
        """Discover using Serper's LinkedIn Google Search."""
        try:
            result = self._discoverer.discover_people(
                company_name=company_name,
                domain=domain,
                limit=limit,
            )

            contacts = [
                DiscoveredContact(
                    first_name=c.first_name,
                    last_name=c.last_name,
                    title=c.title,
                    email="",  # Serper doesn't return emails
                    linkedin_url=c.linkedin_url,
                    management_level=c.management_level,
                    enrichment_source="serper",
                    is_stale=c.is_stale,
                    confidence_score=0.85 if not c.is_stale else 0.60,
                )
                for c in result
                if c.first_name and c.last_name
            ]

            return DiscoveryResult(
                contacts=contacts,
                source="serper",
                total_found=len(contacts),
                credits_used=1,  # Serper charges 1 credit per search
                message=f"Found {len(contacts)} contacts via Serper",
            )
        except Exception as e:
            logger.warning(f"Serper discovery failed: {e}")
            return DiscoveryResult(
                contacts=[],
                source="serper",
                message=f"Serper discovery failed: {e}",
            )


class DiscoveryRegistry:
    """Registry for managing discovery providers.

    Automatically selects the best available provider based on configuration
    and priority. Supports easy addition of new providers.
    """

    def __init__(self, settings):
        self._settings = settings
        self._providers: dict[str, DiscoveryProvider] = {}
        self._initialize_providers()

    def _initialize_providers(self) -> None:
        """Initialize all available providers based on settings."""
        # Apollo provider (highest priority if available)
        if self._settings.apollo_api_key:
            apollo = ApolloDiscoveryProvider(
                api_key=self._settings.apollo_api_key,
                rate_limit=self._settings.apollo_rate_limit,
            )
            self._providers["apollo"] = apollo
            logger.info("Discovery: Apollo provider registered")

        # Serper provider (fallback)
        if self._settings.serper_api_key:
            serper = SerperDiscoveryProvider(api_key=self._settings.serper_api_key)
            self._providers["serper"] = serper
            logger.info("Discovery: Serper provider registered")

    def get_provider(self, name: str | None = None) -> DiscoveryProvider | None:
        """Get a specific provider or auto-select the best one.

        Args:
            name: Specific provider name ("apollo", "serper") or None for auto-select

        Returns:
            DiscoveryProvider or None if not available
        """
        if name:
            provider = self._providers.get(name)
            if provider and provider.is_configured():
                logger.info(f"Discovery: Using specified provider '{name}'")
                return provider
            else:
                logger.warning(f"Discovery: Provider '{name}' not available or not configured")
                return None

        # Auto-select: sort by priority (descending) and return first configured
        sorted_providers = sorted(
            self._providers.values(),
            key=lambda p: p.priority,
            reverse=True,
        )

        for provider in sorted_providers:
            if provider.is_configured():
                logger.info(f"Discovery: Auto-selected provider '{provider.name}'")
                return provider

        logger.warning("Discovery: No discovery providers available")
        return None

    def list_providers(self) -> dict[str, dict]:
        """List all available providers with their status."""
        result = {}
        for name, provider in self._providers.items():
            result[name] = {
                "name": provider.name,
                "configured": provider.is_configured(),
                "priority": provider.priority,
            }
        return result

    def discover(
        self,
        domain: str,
        company_name: str,
        limit: int = 50,
        provider: str | None = None,
    ) -> DiscoveryResult:
        """Discover contacts using specified or best available provider.

        Args:
            domain: Company domain
            company_name: Company name
            limit: Max contacts to return
            provider: Specific provider name or None for auto-select

        Returns:
            DiscoveryResult from the selected provider
        """
        selected = self.get_provider(provider)

        if not selected:
            return DiscoveryResult(
                contacts=[],
                source="none",
                message="No discovery providers configured. Please add Apollo or Serper API key to .env",
            )

        logger.info(f"Discovery: Using {selected.name} to discover contacts for {company_name} ({domain})")

        return selected.discover(
            domain=domain,
            company_name=company_name,
            limit=limit,
        )
