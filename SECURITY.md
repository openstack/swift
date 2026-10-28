# Security Policy

## Reporting a vulnerability

Please report suspected security vulnerabilities in OpenStack Swift privately
so that the OpenStack Vulnerability Management Team (VMT) and Swift security
reviewers can investigate and coordinate disclosure.

To report an issue:

1. Sign in to Launchpad and [report a bug against Swift](https://bugs.launchpad.net/swift/+filebug).
2. Describe the issue and select **“This bug is a security vulnerability”**
   before submitting it. Initially, only the VMT can access the private report.
   The VMT will add Swift security reviewers as needed to investigate it.

If the issue is especially sensitive, or you cannot use Launchpad, email one
or more [VMT members](https://security.openstack.org/vmt.html) directly. Their
current contact details and OpenPGP keys are listed on that page. Encrypting
sensitive reports is encouraged.

See the [OpenStack security reporting instructions](https://security.openstack.org/reporting.html)
for further guidance.

Please include, where available:

- The affected Swift version or commit and component.
- Relevant configuration, including enabled middleware and authentication
  configuration, and any access or privileges needed to trigger the issue.
- Reproduction steps or a minimal proof of concept.
- The expected and observed behavior, potential impact, and any known mitigation.
- Your preferred name or attribution for the report.

Remove credentials and unrelated personal or customer data from attachments.
You do not need a complete analysis or a proposed fix to submit a report.
If you are unsure whether an issue is a vulnerability or which versions are
affected, report it privately so the VMT and Swift security reviewers can help
assess it.

While an issue is being coordinated privately, keep discussion and proposed
patches in the private report, attaching proposed fixes there for review.
Do not post vulnerability details or fixes to public issues, mailing lists,
chat, or Gerrit before coordinated disclosure.

## Supported versions and scope

Swift's published versions are listed on the
[Swift releases page](https://releases.openstack.org/teams/swift.html).
Security support follows OpenStack's
[VMT scope](https://security.openstack.org/repos-overseen.html) and
[branch maintenance policy](https://docs.openstack.org/project-team-guide/stable-branches.html).

VMT-coordinated advisories cover eligible vulnerabilities in official releases
or maintained stable branches. Fixes are coordinated for the development branch
and affected maintained stable branches. Issues found only in unreleased
development code or only on unmaintained branches do not receive VMT-coordinated
disclosure. Prereleases, milestones, and release candidates are not official
releases for this purpose.

For vendor-specific changes or distribution packages, consult the provider's
security and support policies. Previously unknown vulnerabilities in external
dependencies are referred to the relevant upstream security contacts on a
best-effort basis under those projects' policies.

## Coordinated disclosure

Swift follows the
[OpenStack Vulnerability Management Process](https://security.openstack.org/vmt-process.html).
The VMT and Swift security reviewers assess reports, determine affected versions,
and coordinate fixes. Access to private details is limited to people needed to
investigate and resolve the issue.

Depending on the findings, a report may result in an OpenStack Security Advisory
(OSSA), an OpenStack Security Note (OSSN), or ordinary public bug handling.
The VMT coordinates disclosure timing with the reporter and, where appropriate,
advance notification to downstream stakeholders. Reporters retain control over
disclosure of their findings.

Private reports are normally made public within 90 calendar days of receipt,
even if a fix is not available, subject to the unusual exceptions described in
OpenStack's policy. Disclosure may happen sooner, including when significant
details are already public.

Published advisories and security guidance are available from
[OpenStack Security](https://security.openstack.org/).
