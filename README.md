# z4j-huey

[![PyPI version](https://img.shields.io/pypi/v/z4j-huey.svg)](https://pypi.org/project/z4j-huey/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-huey.svg)](https://pypi.org/project/z4j-huey/)
[![License](https://img.shields.io/pypi/l/z4j-huey.svg)](https://github.com/z4jdev/z4j-huey/blob/main/LICENSE)


z4j queue-engine adapter for [Huey](https://github.com/coleifer/huey).

```python
from huey import RedisHuey
from z4j_huey import HueyEngineAdapter

huey = RedisHuey("myapp", host="redis", port=6379)

@huey.task()
def add(x, y):
    return x + y

# In your z4j-bare bootstrap:
from z4j_bare import install_agent
install_agent(engines=[HueyEngineAdapter(huey=huey)])
```

## Capabilities

- ✅ Task discovery (every `@huey.task` and `@huey.periodic_task`)
- ✅ Per-task `retry_task` / `cancel_task`
- ✅ Result-backend reconciliation (`reconcile_task`) - closes the
  "stuck started forever" gap by consulting Huey's result store.
- ✅ Queue/worker introspection
- ❌ `bulk_retry`, `purge_queue` - Huey's storage layer doesn't
  expose the bulk primitives needed; deferred to v1.1.
- ❌ `restart_worker`, `rate_limit` - Huey's consumer model has no
  remote-control channel.

## Periodic tasks

Huey's `@periodic_task(crontab(...))` decorators are first-class.
Pair this adapter with `z4j-hueyperiodic` to surface them on the
Schedules page.

Apache 2.0.

## License

Apache 2.0 - see [LICENSE](LICENSE). This package is deliberately permissively licensed so that proprietary Django / Flask / FastAPI applications can import it without any license concerns.

## Links

- Homepage: <https://z4j.com>
- Documentation: <https://z4j.dev>
- Source: <https://github.com/z4jdev/z4j-huey>
- Issues: <https://github.com/z4jdev/z4j-huey/issues>
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: `security@z4j.com` (see [SECURITY.md](SECURITY.md))
