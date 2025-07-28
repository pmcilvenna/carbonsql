/* SNOWFLAKE OPTIMIZED VERSION - PERFORMANCE FOCUSED */

-- Use result cache for repeated queries
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- Optimize warehouse size for this query
ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 300;

-- Pre-calculate timezone conversion and filtering for better performance
WITH optimized_base AS (
    SELECT /*+ MATERIALIZED */
        demand_tag_id,
        foreign_deal_id,
        -- Pre-calculate timezone conversion once
        DATE_TRUNC('hour', CONVERT_TIMEZONE('UTC', 'America/New_York', ymdh)) AS ymdh_ny,
        -- Pre-calculate all aggregations in single pass
        SUM(cost + platform_fees + data_cost + ivt_fees_estimate) AS billable_cost,
        SUM(CASE WHEN tier_breakout = TRUE THEN impressions ELSE 0 END) AS breakout_impressions,
        SUM(data_cost) AS segment_data_cost,
        SUM(opportunity_timeouts + vast_request_timeouts) AS timeouts,
        SUM(ad_requests) AS total_requests
    FROM vd.demand_full_aggregations
    WHERE account_id = 1  -- Use equality instead of IN for single value
      AND ymdh >= '2025-01-21 00:00:00'
      AND ymdh <= '2025-07-21 01:00:12'
    GROUP BY 
        demand_tag_id,
        foreign_deal_id,
        DATE_TRUNC('hour', CONVERT_TIMEZONE('UTC', 'America/New_York', ymdh))
    HAVING SUM(ad_requests) = 0
)
SELECT /*+ RESULT_CACHE */
    SUM(billable_cost) AS billable_cost,
    SUM(breakout_impressions) AS breakout_impressions,
    SUM(segment_data_cost) AS segment_data_cost,
    SUM(timeouts) AS timeouts,
    SUM(total_requests) AS total_requests,
    demand_tag_id,
    c.code AS campaign_code,
    foreign_deal_id,
    DATE_TRUNC('hour', ymdh_ny) AS ymdh
FROM optimized_base
    INNER JOIN vd.demand_tags AS dt ON optimized_base.demand_tag_id = dt.id
    LEFT OUTER JOIN vd.campaigns AS c ON dt.campaign_id = c.id
GROUP BY 
    DATE_TRUNC('hour', ymdh_ny),
    demand_tag_id,
    campaign_code,
    foreign_deal_id
ORDER BY 
    ymdh ASC,
    total_requests DESC;