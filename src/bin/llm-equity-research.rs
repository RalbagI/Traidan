//! Research-only stdin harness for the LLM equity paper authority.
//!
//! One JSON object is accepted per line. The process keeps one isolated paper
//! portfolio in memory and advances only with the supplied simulated clock. It
//! cannot submit broker orders and exposes no HTTP/authentication surface.

use _rust::llm_equity::LlmEquityDryRunAuthority;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResearchRequest {
    operation: String,
    #[serde(default)]
    authority_state: Option<Value>,
    #[serde(default)]
    decision: Option<Value>,
    #[serde(default)]
    market_research_snapshot: Option<Value>,
    #[serde(default)]
    simulated_received_at: Option<DateTime<Utc>>,
    #[serde(default)]
    corporate_actions: Vec<ResearchCorporateAction>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResearchCorporateAction {
    action_id: String,
    symbol: String,
    action_type: String,
    #[serde(default)]
    ratio: Option<f64>,
    #[serde(default)]
    cash_amount_usd: Option<f64>,
    #[serde(default)]
    new_symbol: Option<String>,
}

fn configuration_from_args() -> Result<(String, Option<PathBuf>), String> {
    let mut arguments = std::env::args().skip(1);
    let mut policy = "forced_aggressive".to_string();
    let mut state_file = None;
    while let Some(flag) = arguments.next() {
        let value = arguments
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--policy" => policy = value,
            "--state-file" => state_file = Some(PathBuf::from(value)),
            _ => return Err("unsupported research harness argument".to_string()),
        }
    }
    Ok((policy, state_file))
}

fn authority_for_policy(policy: &str) -> Result<LlmEquityDryRunAuthority, String> {
    match policy {
        "profit_driven" => Ok(LlmEquityDryRunAuthority::paper_profit_driven()),
        "fully_invested" => Ok(LlmEquityDryRunAuthority::paper_fully_invested()),
        "paper" => Ok(LlmEquityDryRunAuthority::paper()),
        "forced_aggressive" => Ok(LlmEquityDryRunAuthority::paper_forced_aggressive()),
        "llm_discretion" => Ok(LlmEquityDryRunAuthority::paper_llm_discretion()),
        "long_risk_adjusted" => Ok(LlmEquityDryRunAuthority::paper_long_risk_adjusted()),
        _ => Err("unsupported research policy".to_string()),
    }
}

fn response_for_line(
    authority: &mut LlmEquityDryRunAuthority,
    line: &str,
    state_file: Option<&Path>,
    policy: &str,
) -> Value {
    if line.len() > MAX_REQUEST_BYTES {
        return json!({"ok": false, "error": "research request exceeds size limit"});
    }
    let request = match serde_json::from_str::<ResearchRequest>(line) {
        Ok(value) => value,
        Err(error) => return json!({"ok": false, "error": format!("invalid request: {error}")}),
    };
    if request.operation == "mark_portfolio" {
        if request.authority_state.is_some()
            || request.decision.is_some()
            || !request.corporate_actions.is_empty()
        {
            return json!({"ok": false, "error": "mark request shape is invalid"});
        }
        let (Some(snapshot), Some(at)) = (
            request.market_research_snapshot,
            request.simulated_received_at,
        ) else {
            return json!({"ok": false, "error": "mark snapshot and clock are required"});
        };
        let previous = authority.clone();
        return match authority
            .observe_research_snapshot(snapshot, at)
            .and_then(|()| {
                if let Some(path) = state_file {
                    authority.persist_research_to_path(path)?;
                }
                Ok(())
            })
            .and_then(|()| authority.export_research_state())
        {
            Ok(state) => json!({"ok": true, "result": state}),
            Err(error) => {
                *authority = previous;
                json!({"ok": false, "error": error.to_string()})
            }
        };
    }
    if request.operation == "export_state" {
        if request.authority_state.is_some()
            || request.decision.is_some()
            || request.market_research_snapshot.is_some()
            || request.simulated_received_at.is_some()
            || !request.corporate_actions.is_empty()
        {
            return json!({"ok": false, "error": "state export request shape is invalid"});
        }
        return match authority.export_research_state() {
            Ok(state) => json!({"ok": true, "result": state}),
            Err(error) => json!({"ok": false, "error": error.to_string()}),
        };
    }
    if request.operation == "restore_state" {
        if request.decision.is_some()
            || request.market_research_snapshot.is_some()
            || request.simulated_received_at.is_some()
            || !request.corporate_actions.is_empty()
        {
            return json!({"ok": false, "error": "state restore request shape is invalid"});
        }
        let Some(state) = request.authority_state else {
            return json!({"ok": false, "error": "authority state is required"});
        };
        return match LlmEquityDryRunAuthority::restore_research_from_value(state, policy) {
            Ok(restored) => {
                *authority = restored;
                json!({"ok": true, "result": {"restored": true}})
            }
            Err(error) => json!({"ok": false, "error": error.to_string()}),
        };
    }
    if request.operation == "apply_corporate_actions" {
        if request.corporate_actions.is_empty()
            || request.authority_state.is_some()
            || request.decision.is_some()
            || request.market_research_snapshot.is_some()
            || request.simulated_received_at.is_some()
        {
            return json!({"ok": false, "error": "corporate action request shape is invalid"});
        }
        let previous = authority.clone();
        for action in request.corporate_actions {
            if action.action_id.trim().is_empty() {
                *authority = previous;
                return json!({"ok": false, "error": "corporate action id is required"});
            }
            if let Err(error) = authority.apply_research_corporate_action(
                &action.symbol,
                &action.action_type,
                action.ratio,
                action.cash_amount_usd,
                action.new_symbol.as_deref(),
            ) {
                *authority = previous;
                return json!({"ok": false, "error": error.to_string()});
            }
        }
        if let Some(path) = state_file {
            if let Err(error) = authority.persist_research_to_path(path) {
                if !authority.allocation_commit_matches(path) {
                    *authority = previous;
                }
                return json!({"ok": false, "error": error.to_string()});
            }
        }
        return json!({"ok": true, "result": {"portfolio": authority.portfolio.clone()}});
    }
    if request.operation != "apply_cycle"
        || request.authority_state.is_some()
        || !request.corporate_actions.is_empty()
    {
        return json!({"ok": false, "error": "unsupported research operation"});
    }
    let Some(decision_value) = request.decision else {
        return json!({"ok": false, "error": "decision is required"});
    };
    let Some(snapshot_value) = request.market_research_snapshot else {
        return json!({"ok": false, "error": "market snapshot is required"});
    };
    let Some(simulated_received_at) = request.simulated_received_at else {
        return json!({"ok": false, "error": "simulated received time is required"});
    };
    let decision = match serde_json::to_string(&decision_value) {
        Ok(value) => value,
        Err(error) => return json!({"ok": false, "error": format!("invalid decision: {error}")}),
    };
    let snapshot = match serde_json::to_string(&snapshot_value) {
        Ok(value) => value,
        Err(error) => return json!({"ok": false, "error": format!("invalid snapshot: {error}")}),
    };
    let previous = authority.clone();
    match authority.apply_research_json(&decision, &snapshot, simulated_received_at) {
        Ok(result) => {
            if let Some(path) = state_file {
                if let Err(error) = authority.persist_research_to_path(path) {
                    if !authority.allocation_commit_matches(path) {
                        *authority = previous;
                    }
                    return json!({"ok": false, "error": error.to_string()});
                }
            }
            json!({"ok": true, "result": result})
        }
        Err(error) => json!({"ok": false, "error": error.to_string()}),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (policy, state_file) = configuration_from_args().map_err(io::Error::other)?;
    let mut authority = if let Some(path) = state_file.as_deref().filter(|path| path.exists()) {
        LlmEquityDryRunAuthority::restore_research_from_path(path, &policy)?
    } else {
        authority_for_policy(&policy).map_err(io::Error::other)?
    };
    let stdin = io::stdin();
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        serde_json::to_writer(
            &mut stdout,
            &response_for_line(&mut authority, &line, state_file.as_deref(), &policy),
        )?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_policy_without_starting_runtime() {
        assert_eq!(
            authority_for_policy("unknown")
                .expect_err("unknown policy must fail")
                .as_str(),
            "unsupported research policy"
        );
    }

    #[test]
    fn accepts_dedicated_long_risk_adjusted_policy() {
        let authority = authority_for_policy("long_risk_adjusted")
            .expect("research-only long policy is supported");

        assert_eq!(authority.portfolio.cash_usd, 10_000.0);
    }

    #[test]
    fn rejects_unknown_request_fields() {
        let mut authority = LlmEquityDryRunAuthority::paper();
        let response = response_for_line(
            &mut authority,
            r#"{"operation":"apply_cycle","decision":{},"market_research_snapshot":{},"simulated_received_at":"2026-05-28T14:30:01Z","extra":true}"#,
            None,
            "paper",
        );

        assert_eq!(response["ok"], false);
        assert!(response["error"]
            .as_str()
            .unwrap_or_default()
            .contains("unknown field"));
    }

    #[test]
    fn applies_closed_corporate_action_request() {
        let mut authority = LlmEquityDryRunAuthority::paper();
        let response = response_for_line(
            &mut authority,
            r#"{"operation":"apply_corporate_actions","corporate_actions":[{"action_id":"split-1","symbol":"AAPL","action_type":"split","ratio":2.0}]}"#,
            None,
            "paper",
        );

        assert_eq!(response["ok"], true);
        assert_eq!(response["result"]["portfolio"]["cash_usd"], 10_000.0);
    }

    #[test]
    fn exports_and_restores_coordinator_owned_state() {
        let mut authority = LlmEquityDryRunAuthority::paper();
        let exported = response_for_line(
            &mut authority,
            r#"{"operation":"export_state"}"#,
            None,
            "paper",
        );
        let restore_request = json!({
            "operation": "restore_state",
            "authority_state": exported["result"].clone(),
        })
        .to_string();
        let restored = response_for_line(&mut authority, &restore_request, None, "paper");

        assert_eq!(exported["ok"], true);
        assert_eq!(restored, json!({"ok": true, "result": {"restored": true}}));
    }
}

#[cfg(test)]
mod mark_tests {
    use super::*;

    #[test]
    fn research_mark_validates_shape_and_persists_latched_state() {
        let mut authority = authority_for_policy("profit_driven").unwrap();
        for extra in [
            json!({"decision": {}}),
            json!({"authority_state": {}}),
            json!({"corporate_actions": [{"action_id":"a", "symbol":"SPY", "action_type":"split"}]}),
            json!({}),
        ] {
            let mut row = json!({"operation":"mark_portfolio"});
            row.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            assert_eq!(
                response_for_line(&mut authority, &row.to_string(), None, "profit_driven")["ok"],
                false
            );
        }
        let snapshot: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/profit-market-snapshot.json"
        ))
        .unwrap();
        let row = json!({"operation":"mark_portfolio", "market_research_snapshot":snapshot,
                         "simulated_received_at":"2026-05-28T14:30:00Z"});
        let mut seed = authority.export_research_state().unwrap();
        seed["portfolio"]["peak_nav_usd"] = json!(12000.0);
        authority =
            LlmEquityDryRunAuthority::restore_research_from_value(seed, "profit_driven").unwrap();
        let root = std::env::temp_dir().join(format!(
            "profit-mark-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("state.json");
        assert_eq!(
            response_for_line(
                &mut authority,
                &row.to_string(),
                Some(&path),
                "profit_driven"
            )["ok"],
            true
        );
        let saved =
            LlmEquityDryRunAuthority::restore_research_from_path(&path, "profit_driven").unwrap();
        assert_eq!(
            saved.export_research_state().unwrap()["drawdown_halted"],
            true
        );
        assert_eq!(
            response_for_line(&mut authority, &row.to_string(), None, "profit_driven")["ok"],
            true
        );
        let before = authority.export_research_state().unwrap();
        let blocked = path.join("cannot-create.json");
        assert_eq!(
            response_for_line(
                &mut authority,
                &row.to_string(),
                Some(&blocked),
                "profit_driven"
            )["ok"],
            false
        );
        assert_eq!(
            authority.export_research_state().unwrap()["portfolio"],
            before["portfolio"]
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
