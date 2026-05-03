# z4j-huey

[![PyPI version](https://img.shields.io/pypi/v/z4j-huey.svg)](https://pypi.org/project/z4j-huey/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-huey.svg)](https://pypi.org/project/z4j-huey/)
[![License](https://img.shields.io/pypi/l/z4j-huey.svg)](https://github.com/z4jdev/z4j-huey/blob/main/LICENSE)

The Huey engine adapter for [z4j](https://z4j.com).

Streams every Huey task lifecycle event from your consumers to the z4j
brain and accepts operator control actions from the dashboard. Pair
with z4j-hueyperiodic to surface `@periodic_task` schedules.

## What it ships

| Capability | Notes |
|---|---|
| Task lifecycle events | enqueued, started, succeeded, failed, retried, revoked |
| Task discovery | runtime registry merge + static scan |
| Submit / retry / cancel | direct against the Huey instance |
| Bulk retry | filter-driven; re-enqueues matching tasks |
| Purge queue | with confirm-token guard |
| Reconcile task | via Huey's storage introspection |

Supports Huey 2.x and 3.x. Works with the redis, sqlite, and in-memory
storage backends.

## Install

```bash
pip install z4j-huey z4j-hueyperiodic
```

Pair with a framework adapter:

```bash
pip install z4j-django  z4j-huey z4j-hueyperiodic   # Django
pip install z4j-flask   z4j-huey z4j-hueyperiodic   # Flask
pip install z4j-fastapi z4j-huey z4j-hueyperiodic   # FastAPI
pip install z4j-bare    z4j-huey z4j-hueyperiodic   # framework-free worker
```

## Pairs with

- [`z4j-hueyperiodic`](https://github.com/z4jdev/z4j-hueyperiodic), schedule adapter for Huey `@periodic_task`

## Reliability

- No exception from the adapter ever propagates back into Huey hooks
  or your task code.
- Events buffer locally when z4j is unreachable; consumers never
  block on network I/O.

## Documentation

Full docs at [z4j.dev/engines/huey/](https://z4j.dev/engines/huey/).

## License

Apache-2.0, see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-huey/
- Issues: https://github.com/z4jdev/z4j-huey/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
