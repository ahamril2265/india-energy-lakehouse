# Data decisions

Every non-obvious choice about the data, with the evidence behind it.

## DD-01: Conflicting readings for 8–12 July 2019 (Kaggle daily consumption)
- **Finding:** All 33 states have two rows for each of 8–12 July 2019.
  153 of 165 pairs have different values (typically 1–3% apart, up to ~9%).
  The wide source file has the same duplicates, so the issue is in the
  original scrape.
- **Investigated:** No copy matches another week's data (max 12/33 matches,
  consistent with chance for small states). Most likely two overlapping
  weekly reports (provisional and revised) were both captured.
- **Decision:** The 153 conflicting state-days are excluded from silver;
  both copies are kept in ops.conflicting_readings. The 12 identical pairs
  (small states reporting rounded values: Sikkim, Meghalaya, Uttarakhand,
  Puducherry, Arunachal Pradesh, Mizoram, Nagaland, Tripura) are kept once.
- **Consequence:** Aggregates use average daily consumption over valid days
  plus a coverage %, not raw sums.

## DD-02: Gaps in source data
- Kaggle: 498 of 704 calendar days present (~206 missing).
- Mendeley 2024–2025: the file named "January 2024 – June 2025" actually
  ends on 24 June 2025. Within that range all 12,984 hours are present.
- Mendeley 2021–2023 (5 s and 10 s data): every file complete.
- **Decision:** Gaps are not filled or interpolated. Coverage is reported
  alongside aggregates.

## DD-03: Suspicious day-over-day jumps (Kaggle)
- Example: Tamil Nadu 16→17 Jul 2019 (+38%), UP 30→31 Jul 2019 (+54%).
- **Decision:** Flagged in silver (`suspicious_jump`), not removed. Only
  consecutive calendar days are compared.

## DD-04: Statistics block at the end of the 2024–2025 Report sheet
- The last 6 rows are a summary (Minimum, Timestamp, Maximum, Timestamp,
  Average, Sum), not data.
- Sum and Average match the hourly rows. Minimum (0 MW) and Maximum
  (291,851.75 MW), with timestamps such as 17:06:41, do not appear in the
  hourly rows, so they were computed over the underlying high-frequency
  SCADA data.
- **Decision:** Kept in the extract (extract changes shape only);
  quarantined in silver by the valid-timestamp rule.

## DD-05: Hourly values in 2024–2025 may be snapshots, not averages
- 2021–2023 data (5 s and 10 s readings) is averaged to hourly in silver.
- Whether 2024–2025 hourly values are top-of-hour snapshots or hourly
  averages is not documented in the source.
- **Assumption:** Treated as hourly averages. Comparisons of fast-changing
  series (solar, wind) across the 2023/2024 boundary carry this caveat.

## DD-06: Block-level errors in the Kaggle state data
- 25+ dates where 10–17 states change by 30–970% on the same day, in both
  directions.
- Changes are level shifts that persist for days (e.g. Punjab ~90 MU →
  ~290 MU from 2020-01-19, Delhi ~58 → ~130, Haryana ~95 → ~210), not
  one-day spikes, consistent with weekly blocks scraped incorrectly.
- Day-over-day jump flags detect only the first day of each block.
- Real events (Janata Curfew 22 Mar 2020, lockdown from 25 Mar) produce
  similar multi-state changes, so the source alone cannot separate errors
  from events.
- **Decision:** Cross-source reconciliation against official monthly data
  (see DD-07).

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
- **Decision:** Official CEA monthly data is the primary state-level source.
  Kaggle daily data is retained as a documented case study; only
  state-months with status `match` are exposed in gold.

## DD-08: Value-level quality checks for grid readings
- Faults found: gas telemetry negative (2,234 readings, min −5,375 MW);
  demand exactly 0 MW (1,080 readings, 123 hours, 91 days); 2 demand spikes
  (402 GW, 466 GW).
- The first design quarantined whole readings, which discarded valid values
  from other sources and removed 18 hours entirely.
- **Decision:** Only rows without a usable timestamp are quarantined (6).
  Individual values failing a physical check are set to null and logged in
  ops.grid_value_issues. Hourly tables report demand completeness
  separately; hours with <50% demand readings are marked unreliable.

## DD-09: Timestamps are IST wall-clock
- Solar peaks at 12:00 and is ~0 at night, confirming local Indian time.
- **Decision:** Stored as TIMESTAMP_NTZ (no time zone) to prevent shifts.

## DD-10: Official data completeness, fully reconciled
- 1,512 expected state-months (36 entities, Jan 2020 – Jun 2023).
- 1 not in source: Chandigarh, Aug 2022.
- 67 excluded as conflicting duplicates (41 in 2022, 26 in 2023), including
  Himachal Pradesh Jun 2022 with three copies (1,061.04 / 1,070.29 /
  6,428.98).
- 0 failed validity rules. Gold: 1,444 state-months; every bronze row
  accounted for.

## DD-11: Cross-check of official state data against national grid data
- Overlap Sep 2021 – Jun 2023 (22 months); 17 months have complete state
  data.
- Complete months: grid total is +0.88% above the sum of states on average
  (std dev 0.77%, range −0.06% to +2.89%), consistent with inter-state
  transmission losses being included at grid level only.
- Incomplete months are explained by whole-month duplicate publications:
  Jun 2022 (34 conflicting entities) and May 2023 (26).
- **Conclusion:** The two official sources are mutually consistent.
- **Future work:** Use grid totals to choose between duplicate published
  versions in Jun 2022 and May 2023.

## DD-12: Plausibility rule must not remove real shocks
- The first rule (value < 30% of state median) flagged Dadra and Nagar
  Haveli Apr 2020 (155.8 MU vs 752 in Feb), a real lockdown collapse
  followed by gradual recovery (376 in May, 501 in Jun), independently
  visible in the Kaggle data (−90% on 22 Mar 2020).
- **Decision:** A value is implausible only if below 30% of the lower of
  its neighbouring months (fallback: state median). Isolated dips are
  caught; sustained shocks are kept.
- **Evidence the rules complement each other:** Delhi May–Jul 2022 were
  each published twice, once with a ~200 MU value (183.0, 197.7, 203.3)
  alongside the real ~3,800 MU figure, and were excluded by the conflict
  rule. Delhi Aug 2022 had only the wrong value (204.3 MU) and was caught
  by the plausibility rule.
- Jun and Jul 2022 real Delhi values are identical (3,906.44), a likely
  copy-paste in the source.

## DD-13: Night-time solar offset
- Average solar at 00:00–04:00 grows from 0.02 GW (2021)