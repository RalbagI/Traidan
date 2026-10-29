# Public release

This release contains the latest paper-trading application snapshot dated
2026-10-22. Private development history, agent configuration, CI runner paths,
account-specific hosting configuration, and internal research reports are not
part of this distribution. The GitHub repository preserves its existing public
history and adds the audited snapshot as a new commit.

## Setup

Use Python 3.11–3.13, Rust stable, and Node.js 22.12 or newer.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
(cd frontend && npm ci)
```

Follow the README for Alpaca market-data credentials and the local launcher.
Credentials are supplied by each operator through the environment or ignored
local runtime files. No credentials, historical portfolios, account records,
logs, or service-account files are supplied. The system remains paper-only.

Optional monitoring uses Docker Compose. Set a unique `GRAFANA_ADMIN_PASSWORD`
in your environment first. Published monitoring ports bind to loopback only.

Hosted deployments require your own domains and Firebase project. All
`example.com` addresses in deployment documentation are placeholders. Pass
`--url` and `--wss-url` explicitly to the hosted dashboard smoke check.
To permit a hosted dashboard to access the local admin API, set
`TRAIDAN_DASHBOARD_ORIGIN` to its exact HTTPS origin. The API still requires
a bearer token and must remain bound to loopback.

Verification commands are listed in the README. This distribution installs no
automatic startup tasks and includes no automatic deployment workflow.

## Licensing and privacy

Original application code is MIT licensed, copyright 2026 RalbagI. Bundled
Noto fonts and third-party dependencies retain their respective licenses.
Their public attribution and package metadata are intentionally preserved.
Test credentials are synthetic rejection fixtures, never usable credentials.

Before sharing changes, scan the working tree and history with Gitleaks:

```bash
gitleaks dir --redact .
gitleaks git --redact --log-opts=--all .
```

Keep environment files, keys, Terraform state, local runtime state, screenshots,
and logs out of Git. A clean scanner result is a check, not a guarantee; review
new configuration and data files before publishing.
