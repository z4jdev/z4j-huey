"""z4j-huey CLI: ``z4j-huey doctor | check | status | version``."""

from __future__ import annotations

from z4j_bare.cli import make_engine_main

# Huey supports SQLite, Redis, and in-memory backends; no single
# canonical broker env var. Framework doctor checks the resolved
# storage via the huey instance.
main = make_engine_main(
    "huey",
    upstream_package="huey",
    broker_env=None,
)


__all__ = ["main"]
