SELECT
    sum(billable_cost) AS billable_cost,
    sum(breakout_impressions) AS breakout_impressions,
    sum(segment_data_cost) AS segment_data_cost,
    sum(timeouts) AS timeouts,
    sum(total_requests) AS total_requests,
    demand_tag_id,
    c.code AS campaign_code,
    foreign_deal_id,
    date_trunc ('hour', ymdh) AS ymdh
FROM
    (
        SELECT
            sum(
                    (
                        cost + platform_fees + data_cost + ivt_fees_estimate
                        )
            ) AS billable_cost,
            sum(
                    CASE
                        WHEN tier_breakout = True THEN impressions
                        ELSE 0
                        END
            ) AS breakout_impressions,
            sum(data_cost) AS segment_data_cost,
            sum(opportunity_timeouts + vast_request_timeouts) AS timeouts,
            sum(ad_requests) AS total_requests,
            agg_table.demand_tag_id AS demand_tag_id,
            agg_table.foreign_deal_id AS foreign_deal_id,
            date_trunc ('hour', convert_timezone ('UTC', 'America/New_York', ymdh)) AS ymdh
        FROM
            vd.demand_full_aggregations AS agg_table
        WHERE
            agg_table.account_id IN (1)
          AND ymdh >= '2025-01-21 00:00:00'
          AND ymdh <= '2025-07-21 01:00:12'
        GROUP BY
            date_trunc ('hour', convert_timezone ('UTC', 'America/New_York', ymdh)),
            agg_table.demand_tag_id,
            agg_table.foreign_deal_id
        HAVING
            sum(ad_requests) = 0
    ) agg_table
        inner join vd.demand_tags AS dt ON agg_table.demand_tag_id = dt.id
        left outer join vd.campaigns AS c ON dt.campaign_id = c.id
GROUP BY
    date_trunc ('hour', ymdh),
    demand_tag_id,
    campaign_code,
    foreign_deal_id
ORDER BY
    ymdh ASC,
    total_requests DESC
