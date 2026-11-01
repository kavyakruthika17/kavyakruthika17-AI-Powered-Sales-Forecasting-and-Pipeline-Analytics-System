#!/usr/bin/env python3
"""
Synthetic sales pipeline forecasting data generator.
Python 3.10+; standard library only.

Run:
  python sales_forecast_generator.py --out ./sales_data --seed 20261022 --reps 2000 --accounts 2000 --opps 20000

Quick test:
  python sales_forecast_generator.py --out ./starter_data --seed 20261022 --reps 30 --accounts 40 --opps 250

The generator writes CSV, JSON, SQLite SQL seed data, a data dictionary,
a source manifest, validation results, ground truth, and reproducibility checks.
Synthetic data only; no personal or confidential source data is used.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import sqlite3
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

STAGES = [
    {"stage_id": "STG-01", "stage_name": "Prospecting", "probability": 0.05, "order": 1},
    {"stage_id": "STG-02", "stage_name": "Qualification", "probability": 0.15, "order": 2},
    {"stage_id": "STG-03", "stage_name": "Needs Analysis", "probability": 0.30, "order": 3},
    {"stage_id": "STG-04", "stage_name": "Proposal", "probability": 0.50, "order": 4},
    {"stage_id": "STG-05", "stage_name": "Negotiation", "probability": 0.75, "order": 5},
    {"stage_id": "STG-06", "stage_name": "Closed Won", "probability": 1.00, "order": 6},
    {"stage_id": "STG-07", "stage_name": "Closed Lost", "probability": 0.00, "order": 7},
]
STAGE_BY_ID = {x["stage_id"]: x for x in STAGES}
OPEN_STAGES = STAGES[:5]
PRODUCTS = ["Analytics Platform", "Cloud Integration", "Security Suite", "AI Assistant", "Data Warehouse"]
INDUSTRIES = ["Healthcare", "Finance", "Retail", "Manufacturing", "Education", "Technology", "Logistics"]
REGIONS = ["North", "South", "East", "West"]
CURRENCIES = ["USD", "INR", "EUR"]
ACTIVITY_TYPES = ["Call", "Email", "Meeting", "Demo", "Proposal Sent"]
ACTIVITY_OUTCOMES = ["Positive", "Neutral", "No Response", "Follow-up Required"]
QUARTERS = [
    date(2025, 3, 31), date(2025, 6, 30), date(2025, 9, 30), date(2025, 12, 31),
    date(2026, 3, 31), date(2026, 6, 30), date(2026, 9, 30), date(2026, 12, 31),
]
AS_OF = date(2026, 12, 31)
FX_TO_USD = {"USD": 1.0, "INR": 1 / 83.0, "EUR": 1.08}

DATA_DICTIONARY = {
    "SalesRep": {
        "rep_id": "string, required, unique; REP-000001 format",
        "rep_name": "string, required; synthetic name",
        "hire_date": "date, required; <= active_to when active_to exists",
        "seniority": "enum Junior|Mid|Senior, required",
        "active_from": "date, required",
        "active_to": "date|null, optional; >= active_from",
        "territory_id": "FK Territory.territory_id, required",
    },
    "Territory": {
        "territory_id": "string, required, unique; TER-0001 format",
        "territory_name": "string, required, unique",
        "region": "enum North|South|East|West",
        "effective_from": "date, required",
        "effective_to": "date|null, optional",
    },
    "Account": {
        "account_id": "string, required, unique; ACC-000001 format",
        "account_name": "string, required",
        "industry": "enum from INDUSTRIES",
        "employee_band": "enum 1-50|51-200|201-1000|1001+",
        "created_date": "date, required",
        "territory_id": "FK Territory, required",
        "account_status": "enum Active|Inactive",
    },
    "Opportunity": {
        "opportunity_id": "string, required, unique; OPP-YYYY-000001",
        "account_id": "FK Account, required",
        "owner_rep_id": "FK SalesRep, required",
        "created_date": "date, required",
        "close_date": "date, required; normally >= created_date",
        "amount": "decimal, required; > 0, source currency units",
        "currency": "enum USD|INR|EUR",
        "product_ids": "semicolon-separated product IDs; use OpportunityProduct bridge",
        "current_stage_id": "FK Stage, required",
        "status": "enum Open|Closed Won|Closed Lost",
        "issue_type": "string|null; populated only for deliberate anomaly rows",
    },
    "Stage": {
        "stage_id": "string, required, unique",
        "stage_name": "string, required, unique",
        "probability": "decimal [0,1], required",
        "stage_order": "integer, required",
    },
    "StageHistory": {
        "stage_history_id": "string, required, unique; STG-<opportunity-id>-NNN",
        "opportunity_id": "FK Opportunity, required",
        "stage_id": "FK Stage, required",
        "entered_at": "date, required; chronological per opportunity",
        "probability_at_entry": "decimal [0,1]",
        "is_current": "boolean, required; max one true per opportunity",
    },
    "Activity": {
        "activity_id": "string, required, unique; ACT-0000001",
        "opportunity_id": "FK Opportunity, required",
        "rep_id": "FK SalesRep, required",
        "activity_date": "date, required",
        "activity_type": "enum from ACTIVITY_TYPES",
        "outcome": "enum from ACTIVITY_OUTCOMES",
        "duration_minutes": "integer, required; 1..480",
    },
    "Quota": {
        "quota_id": "string, required, unique; QUOTA-<rep-id>-YYYYQn",
        "rep_id": "FK SalesRep, required",
        "quarter": "string YYYYQn, required",
        "quota_amount_usd": "decimal, required; >= 0; USD",
        "effective_from": "date, required",
        "effective_to": "date, required",
    },
    "HistoricalBooking": {
        "booking_id": "string, required, unique; BOOK-0000001",
        "opportunity_id": "FK Opportunity, required, unique",
        "rep_id": "FK SalesRep, required",
        "booking_date": "date, required",
        "amount_usd": "decimal, required; >= 0; normalized USD",
        "source_currency": "enum USD|INR|EUR",
    },
    "ForecastSnapshot": {
        "snapshot_id": "string, required, unique; FCST-YYYY-MM-DD-NNNNNN",
        "opportunity_id": "FK Opportunity, required",
        "snapshot_date": "date, required; fixed quarter end",
        "stage_id": "FK Stage, required; stage known as of snapshot",
        "stage_probability": "decimal [0,1], required; as-of value",
        "amount_usd": "decimal, required; normalized USD",
        "days_open": "integer, required; >= 0",
        "days_to_close": "integer, required; may be negative for overdue deals",
        "activity_count_30d": "integer, required; >= 0",
        "last_activity_age_days": "integer|null, optional",
        "scenario_low_usd": "decimal, required; <= base",
        "scenario_base_usd": "decimal, required",
        "scenario_high_usd": "decimal, required; >= base",
        "actual_outcome": "int|null, optional; deliberately NULL in model input",
        "actual_booking_usd": "decimal|null, optional; deliberately NULL in model input",
    },
    "OpportunityProduct": {
        "opportunity_product_id": "string, required, unique",
        "opportunity_id": "FK Opportunity, required",
        "product_id": "string, required",
        "product_name": "enum from PRODUCTS",
    },
}

def iso(d: date | None) -> str | None:
    return d.isoformat() if d else None

def quarter_key(d: date) -> str:
    return f"{d.year}Q{((d.month - 1) // 3) + 1}"

def q_start(d: date) -> date:
    q = (d.month - 1) // 3
    return date(d.year, q * 3 + 1, 1)

def money(x: float) -> float:
    return round(float(x), 2)

def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def generate(seed: int, n_reps: int, n_accounts: int, n_opps: int, out: Path) -> dict[str, Any]:
    rng = random.Random(seed)
    out.mkdir(parents=True, exist_ok=True)

    # 1. Parent/master data
    territories = []
    for i in range(1, 17):
        territories.append({
            "territory_id": f"TER-{i:04d}", "territory_name": f"Territory {i:02d}",
            "region": REGIONS[(i - 1) % len(REGIONS)],
            "effective_from": "2024-01-01", "effective_to": None,
        })
    reps = []
    for i in range(1, n_reps + 1):
        reps.append({
            "rep_id": f"REP-{i:06d}", "rep_name": f"Sales Rep {i:04d}",
            "hire_date": iso(date(2022, 1, 1) + timedelta(days=rng.randint(0, 1400))),
            "seniority": rng.choices(["Junior", "Mid", "Senior"], [0.35, 0.45, 0.20])[0],
            "active_from": "2024-01-01", "active_to": None,
            "territory_id": f"TER-{rng.randint(1, 16):04d}",
        })
    accounts = []
    for i in range(1, n_accounts + 1):
        accounts.append({
            "account_id": f"ACC-{i:06d}", "account_name": f"Synthetic Account {i:05d}",
            "industry": rng.choice(INDUSTRIES),
            "employee_band": rng.choices(["1-50", "51-200", "201-1000", "1001+"], [0.25, 0.35, 0.28, 0.12])[0],
            "created_date": iso(date(2023, 1, 1) + timedelta(days=rng.randint(0, 1200))),
            "territory_id": f"TER-{rng.randint(1, 16):04d}",
            "account_status": rng.choices(["Active", "Inactive"], [0.94, 0.06])[0],
        })

    stages = [{
        "stage_id": s["stage_id"], "stage_name": s["stage_name"],
        "probability": s["probability"], "stage_order": s["order"]
    } for s in STAGES]

    # 2. Opportunities and their event histories
    opportunities, stage_history, activities, opp_products = [], [], [], []
    latent = {}
    opp_counter = 0
    activity_counter = 0
    for i in range(1, n_opps + 1):
        opp_counter += 1
        oid = f"OPP-2026-{opp_counter:06d}"
        created = date(2025, 1, 1) + timedelta(days=rng.randint(0, 700))
        close = created + timedelta(days=rng.randint(30, 300))
        currency = rng.choices(CURRENCIES, [0.55, 0.35, 0.10])[0]
        amount = money(rng.lognormvariate(9.4, 0.85))
        # Seasonality: Q4 opportunities are slightly larger.
        if close.month in (10, 11, 12):
            amount = money(amount * 1.12)
        rep = rng.choice(reps)
        account = rng.choice(accounts)
        # Latent outcome is generated from a stable probability model, not independently random.
        initial_stage = rng.choices(OPEN_STAGES, [0.25, 0.28, 0.22, 0.16, 0.09])[0]
        activity_signal = rng.random()
        age_factor = max(0.35, 1 - max(0, (AS_OF - created).days - 120) / 900)
        p_win = min(0.92, max(0.03, initial_stage["probability"] * 0.65 + activity_signal * 0.22 + age_factor * 0.10))
        won = rng.random() < p_win
        # Close date / stage lifecycle outcome, grounded in the latent probability.
        final_stage = "STG-06" if won else "STG-07"
        actual_close = min(close, AS_OF) if won else min(close + timedelta(days=rng.randint(0, 90)), AS_OF)
        product_count = 1 if rng.random() < 0.72 else 2
        chosen_products = rng.sample(PRODUCTS, product_count)
        opp_products.extend([{
            "opportunity_product_id": f"OPPPR-{i:06d}-{j:02d}",
            "opportunity_id": oid, "product_id": f"PROD-{PRODUCTS.index(p)+1:03d}",
            "product_name": p
        } for j, p in enumerate(chosen_products, 1)])

        # Current stage is time-dependent for open deals; closed deals retain terminal stage.
        if won or actual_close < AS_OF and not won:
            current_stage = final_stage
            status = "Closed Won" if won else "Closed Lost"
        else:
            current_stage = initial_stage["stage_id"]
            status = "Open"

        opportunities.append({
            "opportunity_id": oid, "account_id": account["account_id"],
            "owner_rep_id": rep["rep_id"], "created_date": iso(created),
            "close_date": iso(close), "amount": amount, "currency": currency,
            "product_ids": ";".join(f"PROD-{PRODUCTS.index(p)+1:03d}" for p in chosen_products),
            "current_stage_id": current_stage, "status": status, "issue_type": None,
        })
        latent[oid] = {
            "won": won, "actual_close": actual_close, "amount_usd": money(amount * FX_TO_USD[currency]),
            "rep_id": rep["rep_id"], "currency": currency, "amount": amount,
            "created": created, "close": close, "initial_stage": initial_stage["stage_id"],
            "activity_signal": activity_signal, "final_stage": final_stage,
        }

        # A short, ordered stage path. Terminal stage is only recorded after outcome.
        path = [s for s in OPEN_STAGES if s["order"] <= STAGE_BY_ID[initial_stage["stage_id"]]["order"]]
        # Stage entries before the current point, with realistic elapsed intervals.
        start_stage = rng.randint(0, len(path) - 1)
        path = path[start_stage:]
        event_date = created
        for k, s in enumerate(path):
            if k:
                event_date += timedelta(days=rng.randint(7, 35))
            if event_date > AS_OF:
                break
            stage_history.append({
                "stage_history_id": f"STG-{oid}-{k+1:03d}", "opportunity_id": oid,
                "stage_id": s["stage_id"], "entered_at": iso(event_date),
                "probability_at_entry": s["probability"], "is_current": False,
            })
        # Keep the terminal/current event after every prior event, even when a
        # synthetic close date falls earlier than the simulated stage progression.
        prior_dates = [date.fromisoformat(h["entered_at"]) for h in stage_history
                       if h["opportunity_id"] == oid]
        last_prior_date = max(prior_dates) if prior_dates else created
        terminal_date = min(AS_OF, max(actual_close, last_prior_date))
        current_event_date = terminal_date if status != "Open" else min(AS_OF, max(last_prior_date, event_date + timedelta(days=20)))
        stage_history.append({
            "stage_history_id": f"STG-{oid}-{len(path)+1:03d}", "opportunity_id": oid,
            "stage_id": final_stage if status != "Open" else current_stage,
            "entered_at": iso(current_event_date),
            "probability_at_entry": STAGE_BY_ID[final_stage if status != "Open" else current_stage]["probability"],
            "is_current": True,
        })

        # Activities are constrained to the opportunity lifetime and snapshot cutoff.
        n_act = rng.randint(0, 14)
        for _ in range(n_act):
            activity_counter += 1
            max_day = max(0, min((AS_OF - created).days, 700))
            ad = created + timedelta(days=rng.randint(0, max_day))
            activities.append({
                "activity_id": f"ACT-{activity_counter:07d}", "opportunity_id": oid,
                "rep_id": rep["rep_id"], "activity_date": iso(ad),
                "activity_type": rng.choice(ACTIVITY_TYPES),
                "outcome": rng.choices(ACTIVITY_OUTCOMES, [0.35, 0.30, 0.20, 0.15])[0],
                "duration_minutes": rng.randint(5, 180),
            })

    # Ensure exactly one current stage-history event per opportunity.
    current_by_opp = {}
    for row in stage_history:
        if row["is_current"]:
            current_by_opp[row["opportunity_id"]] = row
    # 3. Quotas and historical bookings
    quotas, bookings = [], []
    for rep in reps:
        for qdate in QUARTERS:
            q = quarter_key(qdate)
            qamount = money(rng.uniform(120000, 500000))
            quotas.append({
                "quota_id": f"QUOTA-{rep['rep_id']}-{q}", "rep_id": rep["rep_id"],
                "quarter": q, "quota_amount_usd": qamount,
                "effective_from": iso(q_start(qdate)), "effective_to": iso(qdate),
            })
    booking_counter = 0
    for opp in opportunities:
        info = latent[opp["opportunity_id"]]
        if info["won"]:
            booking_counter += 1
            bookings.append({
                "booking_id": f"BOOK-{booking_counter:07d}",
                "opportunity_id": opp["opportunity_id"], "rep_id": info["rep_id"],
                "booking_date": iso(info["actual_close"]),
                "amount_usd": info["amount_usd"], "source_currency": info["currency"],
            })

    # 4. Forecast snapshots. Feature values are computed only from records available by snapshot date.
    snapshots = []
    activity_by_opp = defaultdict(list)
    for a in activities:
        activity_by_opp[a["opportunity_id"]].append(a)
    history_by_opp = defaultdict(list)
    for h in stage_history:
        history_by_opp[h["opportunity_id"]].append(h)
    snapshot_counter = 0
    for qdate in QUARTERS:
        for opp in opportunities:
            oid = opp["opportunity_id"]
            info = latent[oid]
            created = info["created"]
            if created > qdate:
                continue
            # Find the latest stage-history entry known on the snapshot date.
            known = [h for h in history_by_opp[oid] if date.fromisoformat(h["entered_at"]) <= qdate]
            if not known:
                stage_id = "STG-01"
            else:
                stage_id = max(known, key=lambda x: x["entered_at"])["stage_id"]
            # Do not expose final outcome or future booking values in input feature fields.
            if stage_id in ("STG-06", "STG-07"):
                prob = STAGE_BY_ID[stage_id]["probability"]
            else:
                prob = STAGE_BY_ID[stage_id]["probability"]
            amount_usd = info["amount_usd"]
            age = (qdate - created).days
            dclose = (info["close"] - qdate).days
            recent = [a for a in activity_by_opp[oid]
                      if qdate - timedelta(days=30) <= date.fromisoformat(a["activity_date"]) <= qdate]
            past = [a for a in activity_by_opp[oid] if date.fromisoformat(a["activity_date"]) <= qdate]
            last_age = min((qdate - date.fromisoformat(a["activity_date"])).days for a in past) if past else None
            activity_boost = min(0.12, len(recent) * 0.015)
            base_prob = min(0.98, max(0.0, prob + activity_boost))
            # Scenarios are explicit assumptions around conversion probability.
            low = amount_usd * max(0, base_prob - 0.12)
            base = amount_usd * base_prob
            high = amount_usd * min(1, base_prob + 0.12)
            snapshot_counter += 1
            snapshots.append({
                "snapshot_id": f"FCST-{qdate.isoformat()}-{snapshot_counter:06d}",
                "opportunity_id": oid, "snapshot_date": iso(qdate), "stage_id": stage_id,
                "stage_probability": round(prob, 4), "amount_usd": money(amount_usd),
                "days_open": age, "days_to_close": dclose,
                "activity_count_30d": len(recent), "last_activity_age_days": last_age,
                "scenario_low_usd": money(low), "scenario_base_usd": money(base),
                "scenario_high_usd": money(high),
                "actual_outcome": None, "actual_booking_usd": None,
            })

    # 5. Inject labelled anomalies after valid baseline generation.
    ground_truth = []
    if opportunities:
        # Leak anomaly: final outcome inserted into an input snapshot row; ground truth flags it.
        s = snapshots[-1]
        s["actual_outcome"] = int(latent[s["opportunity_id"]]["won"])
        s["actual_booking_usd"] = latent[s["opportunity_id"]]["amount_usd"] if latent[s["opportunity_id"]]["won"] else 0.0
        ground_truth.append({"record_type": "ForecastSnapshot", "record_id": s["snapshot_id"],
                             "issue_type": "FINAL_OUTCOME_LEAKAGE",
                             "expected_decision": "Exclude actual_outcome and actual_booking_usd from model features"})
        # Probability inconsistency.
        s2 = snapshots[max(0, len(snapshots)//3)]
        s2["stage_probability"] = 0.999
        ground_truth.append({"record_type": "ForecastSnapshot", "record_id": s2["snapshot_id"],
                             "issue_type": "STAGE_PROBABILITY_MISMATCH",
                             "expected_decision": "Use stage master probability or quarantine row"})
        # Duplicate counted opportunity snapshot.
        dup = dict(snapshots[0])
        dup["snapshot_id"] = f"FCST-DUP-{dup['snapshot_id']}"
        snapshots.append(dup)
        ground_truth.append({"record_type": "ForecastSnapshot", "record_id": dup["snapshot_id"],
                             "issue_type": "DUPLICATE_OPPORTUNITY_COUNT",
                             "expected_decision": "Deduplicate opportunity within snapshot_date"})
        # Close date before creation.
        opportunities[0]["close_date"] = iso(date.fromisoformat(opportunities[0]["created_date"]) - timedelta(days=3))
        opportunities[0]["issue_type"] = "CLOSE_BEFORE_CREATED"
        ground_truth.append({"record_type": "Opportunity", "record_id": opportunities[0]["opportunity_id"],
                             "issue_type": "CLOSE_BEFORE_CREATED",
                             "expected_decision": "Flag invalid chronology; exclude or repair from source"})
        # Mixed currency aggregation anomaly is represented by an explicit test record.
        ground_truth.append({"record_type": "Aggregation", "record_id": "ANOMALY-MIXED-CURRENCY-001",
                             "issue_type": "MIXED_CURRENCY_TOTAL",
                             "expected_decision": "Normalize every amount to USD before aggregation"})

    # 6. Territory reassignment event: effective-dated territory master version.
    # Add a second effective-dated row for a subset, without changing historical ownership records.
    for idx, rep in enumerate(reps[:max(1, min(20, len(reps)))]):
        old = rep["territory_id"]
        new = f"TER-{(int(old[-4:]) % 16) + 1:04d}"
        if new != old:
            territories.append({
                "territory_id": f"TER-CHG-{idx+1:04d}", "territory_name": f"Territory reassignment {idx+1:02d}",
                "region": REGIONS[(idx + 1) % 4], "effective_from": "2026-07-01", "effective_to": None,
            })

    tables = {
        "SalesRep": reps, "Territory": territories, "Account": accounts, "Stage": stages,
        "Opportunity": opportunities, "StageHistory": stage_history, "Activity": activities,
        "Quota": quotas, "HistoricalBooking": bookings, "ForecastSnapshot": snapshots,
        "OpportunityProduct": opp_products,
    }
    for name, rows in tables.items():
        write_csv(out / f"{name}.csv", rows)

    # Ground truth kept separate from model inputs.
    write_csv(out / "ground_truth.csv", ground_truth)
    (out / "data_dictionary.json").write_text(json.dumps(DATA_DICTIONARY, indent=2), encoding="utf-8")

    # JSON exports (newline-delimited JSON is easy to stream).
    for name, rows in tables.items():
        with (out / f"{name}.jsonl").open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, default=str) + "\n")

    # SQLite SQL seed database, useful for relational querying.
    db_path = out / "sales_forecast_seed.sqlite"
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    for name, rows in tables.items():
        if not rows:
            continue
        cols = list(rows[0].keys())
        # Store all generated values as TEXT for portable CSV parity; cast in analytical queries.
        col_sql = ", ".join('"' + c + '" TEXT' for c in cols)
        conn.execute(f'CREATE TABLE "{name}" ({col_sql})')
        placeholders = ",".join("?" for _ in cols)
        conn.executemany(
            f'INSERT INTO "{name}" VALUES ({placeholders})',
            [[None if r[c] is None else str(r[c]) for c in cols] for r in rows]
        )
    conn.commit()
    conn.close()

    # 7. Validation checks.
    validation = validate(tables)
    (out / "validation_summary.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")

    # 8. Forecast evaluation on clean snapshots only. The intentionally leaked outcome columns are excluded.
    metrics = evaluate(snapshots, latent, ground_truth)
    (out / "forecast_metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    # Manifest with row counts and hashes.
    manifest = {
        "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "seed": seed, "parameters": {"reps": n_reps, "accounts": n_accounts, "opportunities": n_opps},
        "synthetic_only": True, "source_manifest": [],
        "files": {}
    }
    for p in sorted(out.iterdir()):
        if p.is_file() and p.name not in ("source_manifest.json", "reproducibility.json"):
            manifest["files"][p.name] = {"sha256": sha256(p), "bytes": p.stat().st_size}
    (out / "source_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # Reproducibility proof: deterministic table checksums from canonical row serialization.
    canonical = {}
    for name, rows in tables.items():
        payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str).encode()
        canonical[name] = {"rows": len(rows), "sha256": hashlib.sha256(payload).hexdigest()}
    (out / "reproducibility.json").write_text(json.dumps({
        "seed": seed, "canonical_table_checksums": canonical,
        "note": "Run twice with identical arguments and compare canonical_table_checksums."
    }, indent=2), encoding="utf-8")

    return {"tables": {k: len(v) for k, v in tables.items()},
            "validation": validation, "metrics": metrics, "out": str(out)}

def validate(tables: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    errors, warnings, checks = [], [], {}
    pk_fields = {
        "SalesRep": "rep_id", "Territory": "territory_id", "Account": "account_id",
        "Opportunity": "opportunity_id", "Stage": "stage_id", "StageHistory": "stage_history_id",
        "Activity": "activity_id", "Quota": "quota_id", "HistoricalBooking": "booking_id",
        "ForecastSnapshot": "snapshot_id", "OpportunityProduct": "opportunity_product_id",
    }
    for table, pk in pk_fields.items():
        vals = [r[pk] for r in tables[table]]
        dupes = len(vals) - len(set(vals))
        checks[f"{table}.primary_key_unique"] = dupes == 0
        if dupes:
            errors.append(f"{table}: {dupes} duplicate primary keys")
    rep_ids = {r["rep_id"] for r in tables["SalesRep"]}
    terr_ids = {r["territory_id"] for r in tables["Territory"]}
    acct_ids = {r["account_id"] for r in tables["Account"]}
    opp_ids = {r["opportunity_id"] for r in tables["Opportunity"]}
    stage_ids = {r["stage_id"] for r in tables["Stage"]}
    checks["SalesRep.territory_fk"] = all(r["territory_id"] in terr_ids for r in tables["SalesRep"])
    checks["Account.territory_fk"] = all(r["territory_id"] in terr_ids for r in tables["Account"])
    checks["Opportunity.parent_fks"] = all(r["account_id"] in acct_ids and r["owner_rep_id"] in rep_ids and r["current_stage_id"] in stage_ids for r in tables["Opportunity"])
    checks["StageHistory.fks"] = all(r["opportunity_id"] in opp_ids and r["stage_id"] in stage_ids for r in tables["StageHistory"])
    checks["Activity.fks"] = all(r["opportunity_id"] in opp_ids and r["rep_id"] in rep_ids for r in tables["Activity"])
    checks["Quota.rep_fk"] = all(r["rep_id"] in rep_ids for r in tables["Quota"])
    checks["Booking.fks"] = all(r["opportunity_id"] in opp_ids and r["rep_id"] in rep_ids for r in tables["HistoricalBooking"])
    checks["Snapshot.fks"] = all(r["opportunity_id"] in opp_ids and r["stage_id"] in stage_ids for r in tables["ForecastSnapshot"])
    for k, ok in checks.items():
        if not ok:
            errors.append(f"Failed integrity check: {k}")
    bad_dates = [r["opportunity_id"] for r in tables["Opportunity"] if r["close_date"] < r["created_date"]]
    checks["Opportunity.date_order"] = len(bad_dates) == 0
    if bad_dates:
        warnings.append(f"{len(bad_dates)} deliberately labelled close-before-created anomaly row(s)")
    invalid_probs = [r["snapshot_id"] for r in tables["ForecastSnapshot"]
                     if not 0 <= float(r["stage_probability"]) <= 1]
    checks["Snapshot.probability_range"] = len(invalid_probs) == 0
    # Duplicates by opportunity/date are deliberately present as an edge case.
    keys = [(r["opportunity_id"], r["snapshot_date"]) for r in tables["ForecastSnapshot"]]
    dup_snap = len(keys) - len(set(keys))
    checks["Snapshot.opportunity_date_unique"] = dup_snap == 0
    if dup_snap:
        warnings.append(f"{dup_snap} deliberate duplicate opportunity/date snapshot row(s)")
    # Validate stage-history chronology and current event count.
    per_opp = defaultdict(list)
    for h in tables["StageHistory"]:
        per_opp[h["opportunity_id"]].append(h)
    bad_history = 0
    for rows in per_opp.values():
        dates = [r["entered_at"] for r in rows]
        if dates != sorted(dates):
            bad_history += 1
        if sum(bool(r["is_current"]) for r in rows) != 1:
            bad_history += 1
    checks["StageHistory.lifecycle"] = bad_history == 0
    if bad_history:
        warnings.append(f"{bad_history} stage-history lifecycle issue(s)")
    checks["Opportunity.amount_positive"] = all(float(r["amount"]) > 0 for r in tables["Opportunity"])
    return {"passed": sum(bool(v) for v in checks.values()),
            "total_checks": len(checks), "checks": checks, "errors": errors, "warnings": warnings}

def evaluate(snapshots: list[dict[str, Any]], latent: dict[str, dict[str, Any]],
             ground_truth: list[dict[str, Any]]) -> dict[str, Any]:
    flagged = {x["record_id"] for x in ground_truth}
    rows = []
    for s in snapshots:
        if s["snapshot_id"] in flagged or s["actual_outcome"] is not None:
            continue
        info = latent[s["opportunity_id"]]
        # Only evaluate snapshots before outcome date; avoid evaluating future-known terminal results.
        snap_date = date.fromisoformat(s["snapshot_date"])
        if snap_date > info["actual_close"]:
            continue
        y = 1 if info["won"] else 0
        p = float(s["scenario_base_usd"]) / max(1.0, float(s["amount_usd"]))
        rows.append((s, y, min(1.0, max(0.0, p))))
    if not rows:
        return {"n": 0, "note": "No eligible snapshot rows for evaluation."}
    brier = sum((p-y)**2 for _, y, p in rows) / len(rows)
    mae_usd = sum(abs(float(s["scenario_base_usd"]) - (float(s["amount_usd"]) if y else 0.0))
                  for s, y, p in rows) / len(rows)
    by_horizon = defaultdict(list)
    by_stage = defaultdict(list)
    for s, y, p in rows:
        horizon = "0-30" if int(s["days_to_close"]) <= 30 else "31-90" if int(s["days_to_close"]) <= 90 else "90+"
        by_horizon[horizon].append((y, p))
        by_stage[s["stage_id"]].append((y, p))
    def group_metrics(groups):
        result = {}
        for key, vals in groups.items():
            result[key] = {
                "n": len(vals),
                "brier_score": round(sum((p-y)**2 for y, p in vals) / len(vals), 5),
                "observed_win_rate": round(sum(y for y, p in vals) / len(vals), 5),
                "mean_predicted_probability": round(sum(p for y, p in vals) / len(vals), 5),
            }
        return result
    return {
        "n": len(rows), "brier_score": round(brier, 5), "mae_booking_usd": round(mae_usd, 2),
        "by_close_horizon": group_metrics(by_horizon), "by_stage": group_metrics(by_stage),
        "note": "Synthetic outcome labels; evaluate on clean, pre-outcome snapshots only. This is a baseline, not a trained production model."
    }

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic sales forecast data.")
    parser.add_argument("--out", default="./sales_data")
    parser.add_argument("--seed", type=int, default=20261022)
    parser.add_argument("--reps", type=int, default=2000)
    parser.add_argument("--accounts", type=int, default=2000)
    parser.add_argument("--opps", type=int, default=20000)
    args = parser.parse_args()
    result = generate(args.seed, args.reps, args.accounts, args.opps, Path(args.out))
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()
