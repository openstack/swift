---
description: >-
  Git commit message formatting rules for the Swift repository: mandatory
  trailers (Assisted-By, Change-Id, Signed-off-by), trailer ordering and
  preservation, 72-character line wrapping, heredoc quoting, and style
  conventions. Use when creating, amending, or rewording git commits.
alwaysApply: false
---

# Git Commit Message Formatting

Rules for formatting commit messages in the Swift repository. The author
identity comes from the user's personal configuration (e.g. `git config
user.name` / `git config user.email` or an external git-identity rule).


## Commit message trailers

Every commit message must end with a trailer block. The **mandatory** trailers
are (in this relative order, always at the bottom):

```
Assisted-By: <model name>
Change-Id: <generated or preserved Change-Id>
Signed-off-by: <author name> <author email>
```

### Mandatory trailer rules

- Don't add a `Signed-off-by` yourself; the human author must sign off.
  After committing, ask the user to run `git commit --amend -s`.
- Update the model name if the underlying model changes. Use the model name
  without the company name (e.g. `Claude Opus 4.6`, not
  `Anthropic Claude Opus 4.6`).
- `Signed-off-by` is always the last line of the commit message,

### Preserving existing authorship and trailers

When rewording or amending an existing commit, keep its original author
and author date. Do not pass `--author` or `--reset-author`. If the
commit already contains trailers, preserve them:

- **Change-Id:** — always keep the existing value; never regenerate it.
- **Related-Bug:** / **Closes-Bug:** — preserve any `Related-Bug: #...`
  or `Closes-Bug: #...` references.
- **Co-Authored-By:** — preserve any existing `Co-Authored-By:` lines.
  (Do NOT add new ones.)
- **Signed-off-by:** — preserve any existing `Signed-off-by:` lines, but
  never add one.
- **Depends-On:** / **Needed-By:** / **Related-Change:** — preserve any
  cross-change references; Gerrit links any change-ID or URL in them.

Place preserved trailers **above** `Assisted-By`. Lines marked `(optional)`
appear only when the original commit had them:

```
Related-Bug: #<number>                          (optional)
Closes-Bug: #<number>                           (optional)
Assisted-By: <model name>
Co-Authored-By: Name <email>                    (optional)
Change-Id: I<preserved>
Signed-off-by: Other Person <other@example.com> (optional)
```

After the user signs off (the common case):

```
Assisted-By: <model name>
Change-Id: I<generated or preserved>
Signed-off-by: <author name> <author email>
```


## Commit message style

- Limit every line to 72 characters, including the title. Wrap body text at
  word boundaries; break long bullet items and code references across lines
  when needed.
- Wrap method, function, and variable names in single backticks, e.g.
  `get_socket()`, `bind_port`.
- The title may include a lowercase prefix followed by `:` to indicate the
  subsystem or area, e.g. `container:`, `replicator:`, `s3api:`,
  `auditor:`, `ai:`. Only add a prefix when the change is scoped to a
  specific subsystem, or `tests:` when the change is primarily related
  to tests.


## Commit via shell

- Only create commits when the user explicitly asks.
- Rely on the configured git identity; don’t override author or committer.
- Pass the message with a **quoted** heredoc so backticks are not stripped
  by bash command substitution:

```bash
git commit -m "$(cat <<'EOF'
doc: Document `get_socket()` bind settings in the WSGI guide

Explain that `swift.common.wsgi.get_socket()` binds the server
socket from `bind_ip` and `bind_port`, and wraps it with TLS
when `cert_file` is set.

Assisted-By: Opus 5.5
Change-Id: I<generated-or-preserved>
EOF
)"
```

- Then ask the user to sign off with `git commit --amend -s`, which
  appends their `Signed-off-by` as the last line.

- Use `<<'EOF'` (quoted delimiter), not `<<EOF`. Unquoted heredocs treat
  `` `...` `` as command substitution and silently remove backticks from the
  message.
- If the repository's `commit-msg` hook (installed by `git review
  --setup`) is present, let it generate the `Change-Id`; otherwise
  generate one for new commits. Always preserve the original `Change-Id`
  when amending.
