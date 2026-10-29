# QuestDB Migrations

QuestDB migrations live in this directory and are applied through the HTTP SQL
endpoint during deployment or local integration testing.

Apply the LLM equity dry-run schema to a running local QuestDB instance:

```bash
curl -G "http://localhost:9000/exec" \
  --data-urlencode "query=$(cat deployment/questdb/003_llm_equity_dry_run.sql)"
```

The active tables store market research snapshots, LLM decisions, simulated
fills, and memory events. They do not enable broker order submission.

Migration numbering gaps are intentional after the hard pivot. Fresh
deployments should apply the active LLM equity dry-run schema above; existing
dry-run databases should treat it as the new baseline.
