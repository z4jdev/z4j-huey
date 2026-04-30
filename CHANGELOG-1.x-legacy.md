# Changelog

All notable changes to `z4j-huey` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.2] - 2026-04-28

### Added

- **`z4j-huey` console script** + `python -m z4j_huey` module
  form. Both work and dispatch to the same code path. Subcommands:
  - `doctor` - check upstream `huey` library + adapter import + broker URL
  - `check` - alias for doctor
  - `status` - one-line: package presence + broker URL state
  - `version` - print z4j-huey version
  Engines are libraries (no agent runtime to manage), so the CLI is
  intentionally narrower than a framework's: no `run`, no `restart`.
  The framework's doctor (z4j-django, z4j-flask, z4j-fastapi) calls
  into these same probes automatically when huey is the detected
  engine.


## [1.1.0] - 2026-04-28

### Fixed

- **``submit_task`` now actually enqueues on Huey 2.x and 3.x.** Pre-1.1 the adapter called ``callable_obj(*args, **kwargs)`` against the registry entry, which works only if Huey stores the ``TaskWrapper`` decorator in ``_registry._registry``. Both supported Huey lines actually store the underlying ``Task`` **subclass** there, and instantiating the class with task args directly trips ``Task.__init__()`` (which accepts ``args``/``kwargs`` as keyword tuples, not splatted task arguments). The brain-side z4j-scheduler 1.1.0 ``schedule.fire`` dispatcher relies on this method, so the bug surfaced as ``Task.__init__() got an unexpected keyword argument 'X'`` for every Huey-backed scheduled task. New flow: instantiate ``Task(args=..., kwargs=..., eta=..., priority=...)`` then ``huey.enqueue(task_instance)``. Returns ``task_instance.id`` as the engine-native id. Verified end-to-end in docker (huey-app project, 113/113 fires completed at 100%).

### Changed

- Bumped minimum ``z4j-core`` to ``>=1.1.0`` for the v1.1.0 ecosystem family release.

## [1.0.1] - 2026-04-21

### Changed

- Lowered minimum Python version from 3.13 to 3.11. This package now supports Python 3.11, 3.12, 3.13, and 3.14.
- Documentation polish: standardized on ASCII hyphens across README, CHANGELOG, and docstrings for consistent rendering on PyPI.


## [1.0.0] - 2026-04

### Added

<!--
TODO: describe what ships in this first public release. One bullet per
capability. Examples:
- First public release.
- <Headline feature>
- <Second feature>
- N unit tests.
-->

- First public release.

## Links

- Repository: <https://github.com/z4jdev/z4j-huey>
- Issues: <https://github.com/z4jdev/z4j-huey/issues>
- PyPI: <https://pypi.org/project/z4j-huey/>

[Unreleased]: https://github.com/z4jdev/z4j-huey/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/z4jdev/z4j-huey/releases/tag/v1.0.0
