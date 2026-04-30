# z4j-huey

[![PyPI version](https://img.shields.io/pypi/v/z4j-huey.svg)](https://pypi.org/project/z4j-huey/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-huey.svg)](https://pypi.org/project/z4j-huey/)
[![License](https://img.shields.io/pypi/l/z4j-huey.svg)](https://github.com/z4jdev/z4j-huey/blob/main/LICENSE)

The Huey engine adapter for [z4j](https://z4j.com).

Streams Huey task lifecycle events to the z4j brain and accepts
control actions (retry, cancel, bulk retry, purge) from the
dashboard. Pair with z4j-hueyperiodic to surface @periodic_task
schedules.

## Install

```bash
pip install z4j-huey z4j-hueyperiodic
```

## Pairs with

- [`z4j-hueyperiodic`](https://github.com/z4jdev/z4j-hueyperiodic) — schedule adapter for Huey @periodic_task

## Documentation

Full docs at [z4j.dev/engines/huey/](https://z4j.dev/engines/huey/).

## License

Apache-2.0 — see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-huey/
- Issues: https://github.com/z4jdev/z4j-huey/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
