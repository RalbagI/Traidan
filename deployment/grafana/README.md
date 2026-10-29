# Grafana Alerting

The committed Grafana provisioning is safe for local development: dashboards
and alert rules load from git, but alert destinations use the Grafana default contact point
unless an operator configures production routes outside this
repository.

Keep no committed secrets in Grafana files. Slack, PagerDuty, email, or webhook URLs
belong in operator-managed Grafana configuration, environment variables, or
the target Grafana instance UI. They must not be added to dashboard JSON,
provisioned alert-rule YAML, Docker Compose, or checked-in docs.

Production dry-run alerts currently cover backend target health, dashboard
WebSocket message flow, risk breaches, and Firebase Hosting deploy status. The
Firebase deploy health panel expects an external GitLab/Grafana/Prometheus
integration to publish:

```text
traidan_firebase_deploy_success{environment="production",job="firebase-hosting-deploy"} 1
```

Use `1` for the latest successful default-branch deploy and `0` for a failed
deploy. `environment` and `job` are the only approved labels for this metric.
Drop or reject labels whose names contain account, token, secret, or key before
the series reaches Prometheus or Grafana. The metric must not include
`FIREBASE_SERVICE_ACCOUNT_JSON`; that secret must never be pasted into dashboard JSON,
alert labels, annotations, logs, or Grafana contact-point settings.
