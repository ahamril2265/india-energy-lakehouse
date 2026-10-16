# Data decisions

## DD-01: Conflicting readings for 8–12 July 2019 (Kaggle daily consumption)
- **Finding:** All 33 states have two rows for each of 8–12 July 2019.
  153 of 165 pairs have different values (typically 1–3% apart, up to ~9%).
  The wide source file has the same duplicates, so the issue is in the
  original scrape.
- **Investigated:** No copy matches another week's data (max 12/33 matches,
  consistent with chance for small states). Most likely two overlapping
  weekly reports (provisional and revised) were both captured.
- **Decision:** Neither copy can be verified, so these 165 state-days are
  treated as missing in silver. Both copies are kept in
  ops.conflicting_readings.
- **Consequence:** Gold uses average daily consumption over valid days, plus
  a coverage % per month, rather than raw sums.

## DD-02: Gaps in source data
- Kaggle: 498 of 704 calendar days present (~206 missing).
- Mendeley Jan 2024 – Jun 2025: 12,990 of 13,128 hours present (138 missing).
- **Decision:** Gaps are not filled or interpolated. Coverage is reported
  alongside every aggregate.

## DD-03: Suspicious day-over-day jumps
- Example: Tamil Nadu 16→17 Jul 2019 (+38%), UP 30→31 Jul 2019 (+54%).
- **Decision:** Flagged in silver (`suspicious_jump`), not removed.

## DD-04: Statistics block at the end of the 2024–2025 Report sheet
- The last 6 rows are a summary (Minimum, Timestamp, Maximum, Timestamp,
  Average, Sum), not data. Real hourly rows: 12,984 of 13,128 expected
  (144 hours = 6 days missing).
- The 12,984 hourly rows themselves are clean: all on the hour, no
  duplicates, no zero demand, nothing above 250 GW.
- Sum and Average match the hourly rows. Minimum (0 MW) and Maximum
  (291,851.75 MW), with timestamps such as 17:06:41, do not appear in the
  hourly rows, so they were computed over the underlying high-frequency
  SCADA data.
- **Decision:** Summary rows are kept in the extract and quarantined in
  silver by the valid-timestamp rule.

## DD-05: Hourly values in 2024–2025 may be snapshots, not averages
- 2021–2023 data (5 s and 10 s readings) is averaged to hourly in silver.
- Whether the 2024–2025 hourly values are top-of-hour snapshots or hourly
  averages is not documented in the source.
- **Assumption:** Treated as hourly averages. Comparisons of fast-changing
  series (solar, wind) across the 2023/2024 boundary carry this caveat.

  ## DD-06: Block-level errors in the Kaggle state data
- 25+ dates where 10–17 states change by 30–970% on the same day,
  in both directions.
- Changes are level shifts that persist for days (e.g. Punjab ~90 MU →
  ~290 MU from 2020-01-19, Delhi ~58 → ~130, Haryana ~95 → ~210), not
  one-day spikes, consistent with weekly blocks scraped incorrectly.
- Day-over-day jump flags detect only the first day of each block.
- Real events (Janata Curfew 22 Mar 2020, lockdown from 25 Mar) produce
  similar multi-state changes, so the source alone cannot separate
  errors from events.
- **Decision:** Option A, cross-source reconciliation. Kaggle daily data is
  rolled up to state-months and compared with the Ministry of Power's
  official monthly energy met (via Dataful). State-months outside tolerance
  are marked unreliable and excluded from gold analyses.

  ## DD-07: Kaggle state data fails reconciliation against official CEA data
- Official data (CEA via India Data Portal) covers Jan 2020 – Jun 2023;
  no 2019, so Kaggle 2019 cannot be validated.
- 2020: 231 of 396 state-months have <50% Kaggle coverage. Of the 164
  comparable state-months, 78 (48%) are within ±10% of official.
- Errors span whole months, not only flagged days: Punjab Jan 2020 +55%
  with 3 jump days; Delhi +32% to +47% every month Jan–Apr 2020.
- Method check: Dadra and Nagar Haveli shows a consistent −30% offset,
  explained by the official figure including Daman & Diu. States such as
  Jharkhand, Kerala and Bihar agree closely.
- **Decision:** Official CEA monthly data becomes the primary state-level
  source. Kaggle daily data is retained as a documented case study; only
  state-months with status `match` are exposed in gold.

  ## DD-08: Value-level quality checks for grid readings
- Faults found: gas telemetry negative (2,234 readings, min −5,375 MW);
  demand exactly 0 MW (1,080 readings, 123 hours, 91 days); 2 demand spikes
  (402 GW, 466 GW).
- First design quarantined whole readings, which discarded valid values
  from other sources and removed 18 hours entirely.
- **Decision:** Only rows without a usable timestamp are quarantined.
  Individual values failing a physical check are set to null and logged in
  ops.grid_value_issues. Hourly tables report demand completeness separately.

## DD-09: Timestamps are IST wall-clock
- Solar peaks at 12:00 and is ~0 at night, confirming local Indian time.
- **Decision:** Stored as TIMESTAMP_NTZ (no time zone) to prevent shifts.