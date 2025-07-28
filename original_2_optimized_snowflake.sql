/* SNOWFLAKE OPTIMIZED VERSION - PERFORMANCE FOCUSED */

-- Use result cache for repeated queries
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- Optimize warehouse size for this query
ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 300;

-- Pre-materialize the labels subquery for better performance
WITH labels_filter AS (
    SELECT /*+ MATERIALIZED */
        object_id
    FROM labels
    WHERE object_type = 'SupplyTag'
      AND label_id = 2432  -- Use equality instead of IN for single value
),
-- Pre-filter and aggregate the main data with clustering optimization
optimized_base AS (
    SELECT /*+ MATERIALIZED */
        content_id,
        content_title,
        network_name,
        supply_router_id,
        supply_tag_id,
        -- Pre-calculate all aggregations in single pass
        SUM(cost + platform_fees + data_cost + ivt_fees_estimate) AS billable_cost,
        SUM(blocked_requests) AS blocked_requests,
        SUM(CASE WHEN tier_breakout = TRUE THEN js_impressions ELSE 0 END) AS breakout_impressions,
        SUM(CASE 
            WHEN supply_router_id != 0 THEN routed_wo_missed_requests + routed_pm_missed_requests + routed_missed_requests + usable_requests
            ELSE usable_requests + blocked_requests + whiteops_blocked + prebid_blocked_internal_domain + prebid_blocked_internal_ip + prebid_blocked_internal_wo_cache + ss_protected_media_prebid_susp + ss_protected_media_prebid_fraud + prebid_blocked_internal_pm_cache_susp + prebid_blocked_internal_pm_cache_fraud
        END) AS total_requests,
        SUM(usable_requests) AS usable_requests
    FROM vd.supply_content_aggregations
    WHERE account_id = 1260  -- Use equality instead of IN for single value
      AND ymdh >= '2025-07-15 00:00:00'
      AND ymdh <= '2025-07-15 23:59:59'
      AND supply_tag_id IN (SELECT object_id FROM labels_filter)
      AND supply_router_id != 0
    GROUP BY content_id, content_title, network_name, supply_router_id, supply_tag_id
)
SELECT /*+ RESULT_CACHE */
    SUM(billable_cost) AS billable_cost,
    SUM(blocked_requests) AS blocked_requests,
    SUM(breakout_impressions) AS breakout_impressions,
    SUM(total_requests) AS total_requests,
    SUM(usable_requests) AS usable_requests,
    supply_tag_labels.label_name AS supply_tag_label,
    sr.supply_partner_id AS supply_partner_id,
    content_id,
    content_title,
    network_name,
    supply_router_id,
    sr.name AS supply_router
FROM optimized_base
    LEFT JOIN vd.supply_tags AS st ON optimized_base.supply_tag_id = st.id
    LEFT JOIN vd.labels AS supply_tag_labels ON optimized_base.supply_tag_id = supply_tag_labels.object_id
        AND supply_tag_labels.object_type = 'SupplyTag'
        AND supply_tag_labels.label_id = 2432  -- Use equality instead of IN
    LEFT JOIN vd.supply_routers AS sr ON optimized_base.supply_router_id = sr.id
GROUP BY 
    supply_tag_label,
    sr.supply_partner_id,
    content_id,
    content_title,
    network_name,
    supply_router_id,
    supply_router;