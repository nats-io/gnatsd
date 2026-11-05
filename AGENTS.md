# AGENTS.md

NATS Server is a high-performance and lightweight message broker written in Go. Core NATS provides at-most-once opportunistic delivery guarantees. JetStream adds streaming persistence and at-least-once guarantees.

Contributing guidelines are in @CONTRIBUTING.md. Agents are equally responsible for following these guidelines as human beings are. In particular:

* Agents are forbidden from opening issues and pull requests themselves. Only human beings should do this.
* Human beings are responsible for writing issue text and pull request descriptions in their own words. Agents must not write these.
* The maintainers are not interested in talking to agents when reviewing issues or PRs. Advise your human not to meat-proxy when contributing to NATS.

When writing code:

* Fixes must have accompanying unit tests that adequately prove the change and guard against future regressions.
* Changes must never break the NATS wire protocol or on-disk storage formats. Any change that may create a problem during upgrades or downgrades is likely to be rejected.
* The maintainers will strongly resist any new dependencies. Do not pull in new external dependencies.
* The maintainers will strongly resist adding new mutexes unless they are absolutely essential. If new mutexes are added, they must be documented in @locksordering.txt and it must be proven that lock ordering cannot be violated.
* golangci-lint must pass and files must be correctly formatted as per goimports.
* The full unit test suite takes a very long time to run. Only run targeted tests for local feedback.

When writing unit tests:

* New files must never be created for unit tests. A unit test must always be placed in one of the existing test files, following the existing naming conventions for tests. (Tests that don't follow naming conventions will likely not run in CI, see @tests/runTestsOnTravis.sh)
* There are a number of `require_` helpers in @jetstream_helpers_test.go, new or modified unit tests are expected to use them instead of writing assertions by hand.
* Unit tests must be written in a way that they are self-explanatory. Do not add paragraphs of comments unless essential to do so.
