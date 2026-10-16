CREATE OR REFRESH MATERIALIZED VIEW energy.gold.mart_lockdown_impact
COMMENT 'Average daily energy met in April 2020 (national lockdown) vs Jan-Feb 2020 and April 2022'
AS
WITH m AS (
  SELECT s.region, f.state, f.month_start, f.avg_daily_mu
  FROM energy.gold.fact_state_month_energy f
  JOIN energy.gold.dim_state s ON s.state = f.state
  WHERE NOT f.is_implausible AND s.entity_type = 'state_or_ut'
),
p AS (
  SELECT region, state,
         avg(CASE WHEN month_start IN (DATE'2020-01-01', DATE'2020-02-01') THEN avg_daily_mu END) AS jan_feb_2020_mu,
         max(CASE WHEN month_start = DATE'2020-04-01' THEN avg_daily_mu END) AS apr_2020_mu,
         max(CASE WHEN month_start = DATE'2022-04-01' THEN avg_daily_mu END) AS apr_2022_mu
  FROM m
  GROUP BY region, state
)
SELECT *,
       round((apr_2020_mu - jan_feb_2020_mu) / jan_feb_2020_mu * 100, 1) AS pct_vs_jan_feb_2020,
       round((apr_2020_mu - apr_2022_mu) / apr_2022_mu * 100, 1)         AS pct_vs_apr_2022
FROM p;


CREATE OR REFRESH MATERIALIZED VIEW energy.gold.mart_hourly_profile
COMMENT 'Average demand, solar, wind and net demand by hour of day and year (reliable hours only)'
AS
SELECT d.year,
       count(DISTINCT d.month)                                                     AS months_covered,
       f.hour_of_day,
       round(avg(f.demand_mw) / 1000, 2)                                           AS avg_demand_gw,
       round(avg(f.solar_mw) / 1000, 2)                                            AS avg_solar_gw,
       round(avg(f.wind_mw) / 1000, 2)                                             AS avg_wind_gw,
       round(avg(f.demand_mw - coalesce(f.solar_mw, 0) - coalesce(f.wind_mw, 0)) / 1000, 2) AS avg_net_demand_gw,
       round(avg(f.solar_wind_share_pct), 2)                                       AS avg_solar_wind_share_pct,
       count(*)                                                                    AS hours
FROM energy.gold.fact_grid_hourly f
JOIN energy.gold.dim_date d ON d.date_key = f.date_key
WHERE f.demand_reliable
GROUP BY d.year, f.hour_of_day;


CREATE OR REFRESH MATERIALIZED VIEW energy.gold.mart_state_fiscal_year
COMMENT 'Energy met per state and fiscal year, annualised over reported months, with YoY growth'
AS
WITH fy AS (
  SELECT f.state, s.region, d.fiscal_year,
         count(*)              AS months_reported,
         sum(f.energy_met_mu)  AS energy_met_mu,
         sum(f.requirement_mu) AS requirement_mu,
         sum(f.shortfall_mu)   AS shortfall_mu,
         sum(f.days_in_month)  AS days_reported
  FROM energy.gold.fact_state_month_energy f
  JOIN energy.gold.dim_date d  ON d.date_key = f.date_key
  JOIN energy.gold.dim_state s ON s.state = f.state
  WHERE NOT f.is_implausible
  GROUP BY f.state, s.region, d.fiscal_year
),
a AS (
  SELECT *,
         round(energy_met_mu / days_reported * 365, 1)  AS annualised_energy_mu,
         round(shortfall_mu / requirement_mu * 100, 3)  AS shortfall_pct
  FROM fy
)
SELECT *,
       CASE WHEN months_reported >= 10 AND lag(months_reported) OVER w >= 10
            THEN round((annualised_energy_mu - lag(annualised_energy_mu) OVER w)
                       / lag(annualised_energy_mu) OVER w * 100, 2)
       END AS yoy_growth_pct
FROM a
WINDOW w AS (PARTITION BY state ORDER BY fiscal_year);