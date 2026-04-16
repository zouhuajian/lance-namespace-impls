"""Shared helpers for Hive metastore URI configuration and parsing."""

from dataclasses import dataclass
from typing import Mapping
from urllib.parse import urlparse

DEFAULT_HIVE_METASTORE_URI = "thrift://localhost:9083"


@dataclass(frozen=True)
class HiveMetastoreEndpoint:
    """A single parsed Hive metastore endpoint."""

    uri: str
    host: str
    port: int


def get_hive_metastore_uri(
    properties: Mapping[str, object],
    default: str = DEFAULT_HIVE_METASTORE_URI,
) -> str:
    """Read the configured metastore URI string from `uri`."""
    if "uris" in properties:
        raise ValueError(
            "Use `uri` instead of `uris`; `uri` supports comma-separated "
            "metastore addresses."
        )
    return str(properties.get("uri") or default)


def parse_hive_metastore_uris(uri: str) -> list[HiveMetastoreEndpoint]:
    """Parse a single URI or comma-separated URI list into endpoints."""
    raw_uris = [part.strip() for part in uri.split(",") if part.strip()]
    if not raw_uris:
        raise ValueError("Hive metastore URI configuration is empty")

    endpoints = []
    for raw_uri in raw_uris:
        url_parts = urlparse(raw_uri)
        endpoints.append(
            HiveMetastoreEndpoint(
                uri=raw_uri,
                host=url_parts.hostname or "localhost",
                port=url_parts.port or 9083,
            )
        )
    return endpoints
