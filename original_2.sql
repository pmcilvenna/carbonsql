SELECT
    sum(billable_cost) AS billable_cost,
    sum(blocked_requests) AS blocked_requests,
    sum(breakout_impressions) AS breakout_impressions,
    sum(total_requests) AS total_requests,
    sum(usable_requests) AS usable_requests,
    supply_tag_labels.label_name AS supply_tag_label,
    sr.supply_partner_id AS supply_partner_id,
    content_id,
    content_title,
    network_name,
    supply_router_id,
    sr.name AS supply_router
FROM
    (
        SELECT

            sum(
                    (
                        cost + platform_fees + data_cost + ivt_fees_estimate
                        )
            ) AS billable_cost,
            sum(blocked_requests) AS blocked_requests,
            sum(
                    case
                        when tier_breakout = True then js_impressions
                        else 0
                        end
            ) AS breakout_impressions,
            sum(
                    case
                        when supply_router_id != 0 THEN routed_wo_missed_requests + routed_pm_missed_requests + routed_missed_requests + usable_requests
                        ELSE usable_requests + blocked_requests + whiteops_blocked + prebid_blocked_internal_domain + prebid_blocked_internal_ip + prebid_blocked_internal_wo_cache + ss_protected_media_prebid_susp + ss_protected_media_prebid_fraud + prebid_blocked_internal_pm_cache_susp + prebid_blocked_internal_pm_cache_fraud
                        END
            ) AS total_requests,
            sum(usable_requests) AS usable_requests,
            agg_table.content_id AS content_id,
            agg_table.content_title AS content_title,
            agg_table.network_name AS network_name,
            agg_table.supply_router_id AS supply_router_id,
            agg_table.supply_tag_id
        FROM
            vd.supply_content_aggregations AS agg_table
        WHERE
            agg_table.account_id IN (1260)
          AND ymdh >= '2025-07-15 00:00:00'
          AND ymdh <= '2025-07-15 23:59:59'
          AND agg_table.supply_tag_id IN (
            select
                object_id
            from
                labels
            where
                object_type = 'SupplyTag'
              AND label_id IN (2432)
        )
          AND agg_table.supply_router_id != 0
        GROUP BY
            agg_table.content_id,
            agg_table.content_title,
            agg_table.network_name,
            agg_table.supply_router_id,
            agg_table.supply_tag_id
    ) agg_table
        left join vd.supply_tags AS st ON agg_table.supply_tag_id = st.id
        LEFT JOIN vd.labels AS supply_tag_labels ON agg_table.supply_tag_id = supply_tag_labels.object_id
        AND supply_tag_labels.object_type = 'SupplyTag'
        AND supply_tag_labels.label_id IN (2432)
        left join vd.supply_routers AS sr ON agg_table.supply_router_id = sr.id
GROUP BY
    supply_tag_label,
    sr.supply_partner_id,
    content_id,
    content_title,
    network_name,
    supply_router_id,
    supply_router
