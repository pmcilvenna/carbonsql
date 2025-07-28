/* VASTDESK REPORTING ACCOUNT 1197 USER 20777 CACHE ID 1753704000000366a8783149994b120062992747c0a0b */
/* SNOWFLAKE OPTIMIZED VERSION - PERFORMANCE FOCUSED */

-- Use result cache for repeated queries
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- Optimize warehouse size for this query
ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 300;

-- Pre-filter and materialize the base data with clustering optimization
WITH optimized_base AS (
    SELECT /*+ MATERIALIZED */
        demand_tag_id,
        -- Pre-calculate all aggregations in single pass
        SUM(ad_seconds) AS ad_seconds,
        SUM(loads_unfiltered) AS analyzed_impressions,
        SUM(ap_demand_opportunity_seconds) AS ap_demand_opportunity_seconds,
        SUM(ap_slots_seconds_filled) AS ap_slots_seconds_filled,
        SUM(cost + platform_fees + data_cost + ivt_fees_estimate) AS billable_cost,
        SUM(CASE WHEN tier_breakout = TRUE THEN impressions ELSE 0 END) AS breakout_impressions,
        SUM(cdn_cost) AS cdn_cost,
        SUM(clicks) AS clicks,
        SUM(ad_vis_and_aud_on_complete_sum) AS complete_vis_aud_impressions,
        SUM(cost) AS cost,
        SUM(duplicate_impressions) AS duplicate_impressions,
        SUM(errors) AS errors,
        SUM(expired_impressions) AS expired_impressions,
        SUM(first_quartile) AS first_quartile,
        SUM(forensiq_measurable_impressions) AS forensiq_impressions,
        SUM(forensiq_invalid_impressions) AS forensiq_ivt_impressions,
        SUM(fourth_quartile) AS fourth_quartile,
        SUM(has_ads) AS has_ads,
        SUM(ias_ivt_impressions) AS ias_bot_impressions,
        SUM(ias_groupm_viewable_impressions) AS ias_groupm_viewable_impressions,
        SUM(ias_measurable_impressions) AS ias_impressions,
        SUM(ias_mrc_viewable_impressions) AS ias_mrc_viewable_impressions,
        SUM(impressions) AS impressions,
        SUM(measurable_impressions) AS in_view_impressions,
        SUM(ivt_fees_actual) AS ivt_fees,
        SUM(ivt_fees_estimate) AS ivt_fees_estimate,
        SUM(susp_human) AS moat_human_impressions,
        SUM(non_billable_fees) AS non_billable_fees,
        SUM(openrtb_bidder_errors) AS openrtb_bidder_errors,
        SUM(openrtb_bids) AS openrtb_bids,
        SUM(openrtb_bid_requests) AS openrtb_bid_requests,
        SUM(openrtb_wins) AS openrtb_wins,
        SUM(opportunities) AS opportunities,
        SUM(opportunity_response_time) AS opportunity_response_time,
        SUM(platform_fees) AS platform_fees,
        SUM(player_audible_full_vis_half_time_sum) AS player_audible_full_vis_half_time_sum,
        SUM(protected_media_ivt_impressions) AS protected_media_ivt_impressions,
        SUM(protected_media_total_impressions) AS protected_media_total_impressions,
        SUM(revenue) AS revenue,
        SUM(second_quartile) AS second_quartile,
        SUM(data_cost) AS segment_data_cost,
        SUM(starts) AS starts,
        SUM(third_party_fees) AS third_party_fees,
        SUM(third_quartile) AS third_quartile,
        SUM(opportunity_timeouts + vast_request_timeouts) AS timeouts,
        SUM(ad_requests) AS total_requests,
        SUM(two_sec_video_in_view_impressions) AS two_sec_video_in_view_impressions,
        SUM(ad_requests) AS usable_requests,
        SUM(vast_responses) AS vast_responses
    FROM vd.demand_full_aggregations
    WHERE account_id = 1197  -- Use equality instead of IN for single value
      AND ymdh >= '2025-06-29 00:00:00'
      AND country IN ('KZ', 'UZ')
    GROUP BY demand_tag_id
)
SELECT /*+ RESULT_CACHE */
    ad_seconds,
    analyzed_impressions,
    ap_demand_opportunity_seconds,
    ap_slots_seconds_filled,
    billable_cost,
    breakout_impressions,
    cdn_cost,
    clicks,
    complete_vis_aud_impressions,
    cost,
    duplicate_impressions,
    errors,
    expired_impressions,
    first_quartile,
    forensiq_impressions,
    forensiq_ivt_impressions,
    fourth_quartile,
    has_ads,
    ias_bot_impressions,
    ias_groupm_viewable_impressions,
    ias_impressions,
    ias_mrc_viewable_impressions,
    impressions,
    in_view_impressions,
    ivt_fees,
    ivt_fees_estimate,
    moat_human_impressions,
    non_billable_fees,
    openrtb_bidder_errors,
    openrtb_bids,
    openrtb_bid_requests,
    openrtb_wins,
    opportunities,
    opportunity_response_time,
    platform_fees,
    player_audible_full_vis_half_time_sum,
    protected_media_ivt_impressions,
    protected_media_total_impressions,
    revenue,
    second_quartile,
    segment_data_cost,
    starts,
    third_party_fees,
    third_quartile,
    timeouts,
    total_requests,
    two_sec_video_in_view_impressions,
    usable_requests,
    vast_responses,
    demand_tag_id
FROM optimized_base
ORDER BY total_requests DESC;