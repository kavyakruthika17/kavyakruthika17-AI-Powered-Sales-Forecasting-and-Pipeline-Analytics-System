# Synthetic Sales Forecasting Dataset

## Run

Requires Python 3.10+ and no third-party packages.

```bash
python sales_forecast_generator.py --out ./sales_data --seed 20261022 --reps 2000 --accounts 2000 --opps 20000
```

Small test run:

```bash
python sales_forecast_generator.py --out ./starter_data --seed 20261022 --reps 30 --accounts 40 --opps 250
```

## Outputs

- One CSV and one newline-delimited JSON (`.jsonl`) per entity.
- `OpportunityProduct` bridge table for the opportunity/product many-to-many relationship.
- `ground_truth.csv` contains deliberate anomaly labels and expected handling; keep it out of model features.
- `data_dictionary.json` describes field types, constraints, enums, and key rules.
- `sales_forecast_seed.sqlite` contains the generated tables for SQL analysis.
- `validation_summary.json` reports key, foreign-key, lifecycle, chronology, and duplicate checks.
- `forecast_metrics.json` reports Brier score, booking MAE, and segment metrics.
- `source_manifest.json` records seed, parameters, file hashes, and provenance.
- `reproducibility.json` contains canonical per-table checksums. Run twice with identical arguments and compare checksums.

## Modeling notes

The snapshot's `scenario_base_usd` is the probability-weighted booking forecast. Low/base/high are sensitivity scenarios created by subtracting/adding 12 percentage points to the stage probability plus recent activity uplift, bounded to [0, 1]. This is intentionally transparent and can be changed in the generator.

Do not use `actual_outcome` or `actual_booking_usd` as model inputs. They are null in normal snapshot rows; one deliberately labelled leakage row is injected for testing. Exclude any `ground_truth.csv` rows and flagged IDs from model training/evaluation.

`HistoricalBooking.amount_usd` and `ForecastSnapshot.amount_usd` are normalized to USD using fixed illustrative conversion assumptions: USD=1, INR=1/83, EUR=1.08. These are synthetic constants, not live FX rates.

The synthetic outcome is generated once from stage, activity signal, and age factor, then reused for bookings and evaluation labels. The same opportunity does not receive independently randomized labels in each table.

## Relationships / cardinality

- Territory 1:N SalesRep and Territory 1:N Account (effective-dated territory records are represented in the territory table).
- Account 1:N Opportunity.
- SalesRep 1:N Opportunity; SalesRep 1:N Activity; SalesRep 1:N Quota; SalesRep 1:N HistoricalBooking.
- Opportunity 1:N StageHistory, Activity, ForecastSnapshot, HistoricalBooking (0:1 for booking).
- Stage 1:N StageHistory and Stage 1:N ForecastSnapshot.
- Opportunity N:M Product, implemented with OpportunityProduct bridge.

## Important implementation caveat

This is a synthetic-data starter project, not a fully calibrated revenue forecasting model. For production use, build point-in-time feature extraction from event timestamps, add real historical outcome data with lawful access, train and backtest by rolling forecast date, and calibrate probabilities by segment. Territory changes should be modeled as effective-dated assignment events in a production schema rather than inferred from the current rep master.
