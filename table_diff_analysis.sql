-- =============================================================================
-- TABLE METRICS DIFF ANALYSIS - MODE DASHBOARD
-- =============================================================================
-- Comparing: etl.agg_paid_marketing_campaign_performance (OLD)
--        vs: etl.agg_paid_marketing_campaign_performance_new (NEW)
--
-- Key Metrics:
--   - Orders (num_placed_orders)
--   - GTV (amt_total_gtv)
--   - iGTV (amt_total_igtv)
--   - Incremental Orders (num_incremental_placed_orders)
--
-- Mode Parameters:
--   {{start_date}} - Default: 2025-01-01
--   {{end_date}}   - Default: Current date
-- =============================================================================


-- =============================================================================
-- QUERY 1: AGGREGATE SUMMARY
-- =============================================================================
-- Use this for KPI cards or summary table showing overall differences

WITH old_agg AS (
    SELECT
        SUM(num_placed_orders) AS orders,
        SUM(amt_total_gtv) AS gtv,
        SUM(amt_total_igtv) AS igtv,
        SUM(num_incremental_placed_orders) AS incr_orders
    FROM etl.agg_paid_marketing_campaign_performance
    WHERE network_event_date_pt >= '{{start_date}}'
      AND network_event_date_pt < '{{end_date}}'
),

new_agg AS (
    SELECT
        SUM(num_placed_orders) AS orders,
        SUM(amt_total_gtv) AS gtv,
        SUM(amt_total_igtv) AS igtv,
        SUM(num_incremental_placed_orders) AS incr_orders
    FROM etl.agg_paid_marketing_campaign_performance_new
    WHERE network_event_date_pt >= '{{start_date}}'
      AND network_event_date_pt < '{{end_date}}'
)

SELECT
    -- Orders
    ROUND(o.orders, 0) AS "Orders (Old)",
    ROUND(n.orders, 0) AS "Orders (New)",
    ROUND(n.orders - o.orders, 0) AS "Orders Diff",
    ROUND(((n.orders - o.orders) / NULLIF(o.orders, 0)) * 100, 1) AS "Orders % Diff",
    
    -- GTV
    ROUND(o.gtv / 1e6, 2) AS "GTV Old ($M)",
    ROUND(n.gtv / 1e6, 2) AS "GTV New ($M)",
    ROUND((n.gtv - o.gtv) / 1e6, 2) AS "GTV Diff ($M)",
    ROUND(((n.gtv - o.gtv) / NULLIF(o.gtv, 0)) * 100, 1) AS "GTV % Diff",
    
    -- iGTV
    ROUND(o.igtv / 1e6, 2) AS "iGTV Old ($M)",
    ROUND(n.igtv / 1e6, 2) AS "iGTV New ($M)",
    ROUND((n.igtv - o.igtv) / 1e6, 2) AS "iGTV Diff ($M)",
    ROUND(((n.igtv - o.igtv) / NULLIF(o.igtv, 0)) * 100, 1) AS "iGTV % Diff",
    
    -- Incremental Orders
    ROUND(o.incr_orders, 0) AS "Incr Orders (Old)",
    ROUND(n.incr_orders, 0) AS "Incr Orders (New)",
    ROUND(n.incr_orders - o.incr_orders, 0) AS "Incr Orders Diff",
    ROUND(((n.incr_orders - o.incr_orders) / NULLIF(o.incr_orders, 0)) * 100, 1) AS "Incr Orders % Diff"

FROM old_agg o
CROSS JOIN new_agg n
;


-- =============================================================================
-- QUERY 2: BY PAID CHANNEL
-- =============================================================================
-- Use this for bar charts or detailed table by channel

WITH old_channel AS (
    SELECT
        paid_channel,
        SUM(num_placed_orders) AS orders,
        SUM(amt_total_gtv) AS gtv,
        SUM(amt_total_igtv) AS igtv,
        SUM(num_incremental_placed_orders) AS incr_orders
    FROM etl.agg_paid_marketing_campaign_performance
    WHERE network_event_date_pt >= '{{start_date}}'
      AND network_event_date_pt < '{{end_date}}'
    GROUP BY paid_channel
),

new_channel AS (
    SELECT
        paid_channel,
        SUM(num_placed_orders) AS orders,
        SUM(amt_total_gtv) AS gtv,
        SUM(amt_total_igtv) AS igtv,
        SUM(num_incremental_placed_orders) AS incr_orders
    FROM etl.agg_paid_marketing_campaign_performance_new
    WHERE network_event_date_pt >= '{{start_date}}'
      AND network_event_date_pt < '{{end_date}}'
    GROUP BY paid_channel
)

SELECT
    COALESCE(o.paid_channel, n.paid_channel) AS "Paid Channel",
    
    -- Orders
    ROUND(o.orders, 0) AS "Orders Old",
    ROUND(n.orders, 0) AS "Orders New",
    ROUND(((n.orders - o.orders) / NULLIF(o.orders, 0)) * 100, 1) AS "Orders % Diff",
    
    -- GTV (in millions)
    ROUND(o.gtv / 1e6, 2) AS "GTV Old ($M)",
    ROUND(n.gtv / 1e6, 2) AS "GTV New ($M)",
    ROUND(((n.gtv - o.gtv) / NULLIF(o.gtv, 0)) * 100, 1) AS "GTV % Diff",
    
    -- iGTV (in millions)
    ROUND(o.igtv / 1e6, 2) AS "iGTV Old ($M)",
    ROUND(n.igtv / 1e6, 2) AS "iGTV New ($M)",
    ROUND(((n.igtv - o.igtv) / NULLIF(o.igtv, 0)) * 100, 1) AS "iGTV % Diff",
    
    -- Incremental Orders
    ROUND(o.incr_orders, 0) AS "Incr Orders Old",
    ROUND(n.incr_orders, 0) AS "Incr Orders New",
    ROUND(((n.incr_orders - o.incr_orders) / NULLIF(o.incr_orders, 0)) * 100, 1) AS "Incr Orders % Diff"

FROM old_channel o
FULL OUTER JOIN new_channel n 
    ON o.paid_channel = n.paid_channel
ORDER BY COALESCE(n.orders, 0) DESC
;


-- =============================================================================
-- QUERY 3: WEEKLY TIMESERIES
-- =============================================================================
-- Use this for line charts showing trends over time
-- Includes filter columns: year_month, month_name for Mode slicers

WITH old_weekly AS (
    SELECT
        DATE_TRUNC('week', network_event_date_pt::DATE) AS week_start,
        SUM(num_placed_orders) AS orders,
        SUM(amt_total_gtv) AS gtv,
        SUM(amt_total_igtv) AS igtv,
        SUM(num_incremental_placed_orders) AS incr_orders
    FROM etl.agg_paid_marketing_campaign_performance
    WHERE network_event_date_pt >= '{{start_date}}'
      AND network_event_date_pt < '{{end_date}}'
    GROUP BY DATE_TRUNC('week', network_event_date_pt::DATE)
),

new_weekly AS (
    SELECT
        DATE_TRUNC('week', network_event_date_pt::DATE) AS week_start,
        SUM(num_placed_orders) AS orders,
        SUM(amt_total_gtv) AS gtv,
        SUM(amt_total_igtv) AS igtv,
        SUM(num_incremental_placed_orders) AS incr_orders
    FROM etl.agg_paid_marketing_campaign_performance_new
    WHERE network_event_date_pt >= '{{start_date}}'
      AND network_event_date_pt < '{{end_date}}'
    GROUP BY DATE_TRUNC('week', network_event_date_pt::DATE)
)

SELECT
    -- Date columns for filtering
    COALESCE(o.week_start, n.week_start)::DATE AS "Week Start",
    TO_CHAR(COALESCE(o.week_start, n.week_start), 'YYYY-MM') AS "Year Month",
    TO_CHAR(COALESCE(o.week_start, n.week_start), 'Mon YYYY') AS "Month Name",
    YEAR(COALESCE(o.week_start, n.week_start)) AS "Year",
    QUARTER(COALESCE(o.week_start, n.week_start)) AS "Quarter",
    
    -- Orders
    ROUND(o.orders, 0) AS "Orders Old",
    ROUND(n.orders, 0) AS "Orders New",
    ROUND(((n.orders - o.orders) / NULLIF(o.orders, 0)) * 100, 1) AS "Orders % Diff",
    
    -- GTV (in millions)
    ROUND(o.gtv / 1e6, 1) AS "GTV Old ($M)",
    ROUND(n.gtv / 1e6, 1) AS "GTV New ($M)",
    ROUND(((n.gtv - o.gtv) / NULLIF(o.gtv, 0)) * 100, 1) AS "GTV % Diff",
    
    -- iGTV (in millions)
    ROUND(o.igtv / 1e6, 1) AS "iGTV Old ($M)",
    ROUND(n.igtv / 1e6, 1) AS "iGTV New ($M)",
    ROUND(((n.igtv - o.igtv) / NULLIF(o.igtv, 0)) * 100, 1) AS "iGTV % Diff",
    
    -- Incremental Orders
    ROUND(o.incr_orders, 0) AS "Incr Orders Old",
    ROUND(n.incr_orders, 0) AS "Incr Orders New",
    ROUND(((n.incr_orders - o.incr_orders) / NULLIF(o.incr_orders, 0)) * 100, 1) AS "Incr Orders % Diff"

FROM old_weekly o
FULL OUTER JOIN new_weekly n 
    ON o.week_start = n.week_start
ORDER BY "Week Start"
;
