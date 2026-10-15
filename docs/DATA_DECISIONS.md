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