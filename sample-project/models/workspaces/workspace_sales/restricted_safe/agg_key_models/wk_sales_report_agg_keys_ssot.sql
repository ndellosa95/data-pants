-- final requirement pending

{{ config(alias='report_agg_keys_ssot') }}
-- based on report_agg_keys_base model
-- accommodate FY24 SSOT target level reporting keys

-- grains include (pending):
-- business_unit, segment, geo, region, area, sqs, ot, deal_category, deal_group,


WITH report_agg_keys_base AS (

    SELECT *
    FROM {{ref('wk_sales_report_agg_keys_base')}}
)

SELECT DISTINCT
    report_opportunity_user_business_unit,
    report_opportunity_user_segment,
    report_opportunity_user_geo,
    report_opportunity_user_region,
    report_opportunity_user_area,

    -- Calculated reporting fields
    report_opportunity_user_sub_business_unit,
    report_opportunity_user_division,
    report_opportunity_user_asm,

    sales_qualified_source,
    deal_group,

    key_segment,
    key_sqs,
    key_ot,
    key_segment_sqs,
    key_segment_ot,
    key_segment_geo,
    key_segment_geo_sqs,
    key_segment_geo_ot,
    key_segment_geo_region,
    key_segment_geo_region_sqs,
    key_segment_geo_region_ot,
    key_segment_geo_region_area,
    key_segment_geo_region_area_sqs,
    key_segment_geo_region_area_ot,
    key_segment_geo_area,
    sales_team_cro_level,
    sales_team_rd_asm_level,
    sales_team_vp_level,
    sales_team_avp_rd_level,
    sales_team_asm_level,

    --fy24 key
    report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot,

    key_bu_segment_geo_region_area_ot,
    key_bu_segment_geo_region_area_sqs,
    key_bu_segment_geo_region_area_ot_sqs,
     
    key_bu,
    key_bu_ot,
    key_bu_sqs,
    key_bu_subbu,
    key_bu_subbu_ot,
    key_bu_subbu_sqs,
    key_bu_subbu_division,
    key_bu_subbu_division_ot,
    key_bu_subbu_division_sqs,
    key_bu_subbu_division_asm,

    --fy25 keys
    key_geo,

    key_geo_ot,
    key_geo_sqs,
        
    key_geo_bu,
    key_geo_bu_ot,
    key_geo_bu_sqs,

    key_geo_bu_region,
    key_geo_bu_region_ot,
    key_geo_bu_region_sqs,

    key_geo_bu_region_area,
    key_geo_bu_region_area_segment
 


FROM report_agg_keys_base