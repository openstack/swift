# AGENTS.md — agent routing index

Agents: explore the repo directly; this file is a routing index, not a contributor guide.

## Workflow

**Session memory:** Write plans, notes, and ephemeral files to `.tmp/`
(gitignored) rather than the system temporary directory.

**For non-trivial planning**, inspect deps and tooling:
`pyproject.toml` · `tox.ini` · `requirements.txt` · `test-requirements.txt`

**Tests**: Swift uses `pytest`, commonly through `tox`.
- Unit tests: `tox -e py3`
- S3 API tests: `tox -e s3api`
- Lint and static checks: `tox -e pep8`
- Functional tests: `tox -e func`
- EC functional tests: `tox -e func-ec`

## Routing

- Repo layout and architecture: [README.rst](README.rst)
- Contributor entry point: [CONTRIBUTING.rst](CONTRIBUTING.rst)
- Swift contributor guide: [contributing.rst](doc/source/contributor/contributing.rst)
- Code review guidelines: [REVIEW_GUIDELINES.rst](REVIEW_GUIDELINES.rst)
- Development environment: [development_saio.rst](doc/source/development_saio.rst)
- Testing and linting: [tox.ini](tox.ini), [.unittests](.unittests), [.functests](.functests), [.probetests](.probetests)
- Dependencies and packaging: [requirements.txt](requirements.txt), [test-requirements.txt](test-requirements.txt), [bindep.txt](bindep.txt), [setup.cfg](setup.cfg)
- Documentation: [doc/source/](doc/source/)

### Development

- Architecture: [overview_architecture.rst](doc/source/overview_architecture.rst)
- Replication: [overview_replication.rst](doc/source/overview_replication.rst)
- Container sharding: [overview_container_sharding.rst](doc/source/overview_container_sharding.rst)
- Erasure coding: [overview_erasure_code.rst](doc/source/overview_erasure_code.rst)
- Rings and placement: [overview_ring.rst](doc/source/overview_ring.rst)
- Storage policies: [overview_policies.rst](doc/source/overview_policies.rst)

## Project Links

- Repository: https://opendev.org/openstack/swift (GitHub is a mirror only)
- Bug tracking: https://bugs.launchpad.net/swift
- Code review: https://review.opendev.org/q/project:openstack/swift
- Documentation: https://docs.openstack.org/swift/latest/
- API reference: https://docs.openstack.org/api-ref/object-store/

## Rules and skills

- Git commit messages: [.agents/rules/git-commit-messages.md](.agents/rules/git-commit-messages.md) (rule)

## Guardrails

- **Tools:** Do not install missing tools with a package manager or `pip`
- **Concurrency:** Do not introduce new eventlet usage.
  Swift is transitioning toward native threading.
- **Review:** Swift uses Gerrit, not GitHub PRs. Series are always
  unsquashed; each commit must be independently testable and correct.
- **Git:** Read-only operations (`git log`, `git diff`, `git status`) are fine.
  Do not run mutating operations (`add`, `commit`, `reset`, `checkout`, `push`,
  `stash`, `merge`, `rebase`, etc.) unless explicitly instructed to do so.
- **Scope:** Do not reformat or otherwise change lines of code that are not
  directly part of the task at hand. If a line of code or comment does not need to be changed functionally, do not touch it at all.
- **Utility Reuse:** Before writing a new helper, search `swift/common/utils/`
 for an existing function that already does the job, and use it if it fits.
- **API Compatibility:** For S3 API changes, consider AWS S3 compatibility and edge cases.