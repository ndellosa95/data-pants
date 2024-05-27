{{ config(
    materialized="table",
) }}

{{ simple_cte([
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('dim_crm_opportunity','dim_crm_opportunity'),
    ('sheetload_map_ramp_deals','sheetload_map_ramp_deals'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_charge', 'dim_charge'),
    ('fct_charge','fct_charge'),
    ('dim_billing_account','dim_billing_account'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_crm_user', 'dim_crm_user'),
    ('dim_date', 'dim_date')


]) }}

----All Subscriptions from Source that include RampID and Legacy RampID
, dim_subscription_source AS (

  SELECT distinct
    sub.accountid                    AS dim_crm_account_id,
    sub.id                           AS subscription_id,
    sub.name                         AS subscription_name,
    sub.version                      AS subscription_version,
    sub.status                       AS subscription_status,
    sub.termstartdate                AS term_start_date,
    sub.termenddate                  AS term_end_date,
    rampid                           AS ramp_id, ---Identifies ramps booked via current Ramp functionality
    CASE when sub.rampid <> '' OR sub.rampid IS NOT NULL THEN rampid
    ELSE 'Not a ramp' END            AS is_ramp,
    MULTIYEARDEALSUBSCRIPTIONLINKAGE__C AS myb_opportunity_id,---Equivalent to SSP ID in SF, deprecated now, used for identifying Legacy ramps
    sub.opportunityid__c                AS opportunity_id
  FROM {{ source('zuora', 'subscription') }} sub

---Legacy Zuora Ramps 
---Historical Ramp Deals for data >= Sep 2021
---myb_opportunity_id should have a value of SSP_ID
), zuora_legacy_ramps AS (

    SELECT 
      dim_crm_account_id,
      subscription_id,
      subscription_name,
      subscription_version,
      subscription_status,
      term_start_date,
      term_end_date,
      ramp_id,
      is_ramp,
      myb_opportunity_id,
      opportunity_id
    FROM dim_subscription_source
    WHERE 
      myb_opportunity_id != '' 
      AND myb_opportunity_id IS NOT NULL 
      AND myb_opportunity_id!= 'Not a ramp' 
      AND (is_ramp = '' OR is_ramp IS NULL)
      

---Current Ramps from Zuora Ramps Functionality
), zuora_ramps AS (

    SELECT 
      dim_crm_account_id,
      subscription_id,
      subscription_name,
      subscription_version,
      subscription_status,
      term_start_date,
      term_end_date,
      ramp_id,
      is_ramp,
      myb_opportunity_id,
      opportunity_id
    FROM dim_subscription_source
      WHERE ramp_id <> '' 
      AND ramp_id IS NOT NULL
      AND ramp_id <> 'Not a ramp'


--- Legacy SF Ramps 
--- Historical Ramp Deals for data <= October 2021
), sheetload_map_ramp_deal AS (

  SELECT  * 
  FROM 
   sheetload_map_ramp_deals
    WHERE "Overwrite_SSP_ID" IS NOT NULL


--Identifying Ramp Deals from SF by using Opportunity_category
--Opportunity_category is manually updated in SF, over 90% accuracy rate
), ramp_deals AS (

   SELECT 
      mart_crm_opportunity.dim_crm_opportunity_id,
      mart_crm_opportunity.ssp_id, 
      dim_crm_opportunity.opportunity_term	
    FROM mart_crm_opportunity		
    INNER JOIN dim_crm_opportunity			
      ON LEFT(dim_crm_opportunity.dim_crm_opportunity_id,15) = LEFT(mart_crm_opportunity.ssp_id,15)				
    WHERE ssp_id IS NOT NULL 
      AND mart_crm_opportunity.opportunity_category LIKE '%Ramp Deal%'


---Combining All Ramp deals from SF and Zuora sources
), ramp_deals_ssp_id_multiyear_linkage AS (

    SELECT 
      zuora_ramps.subscription_name,
      dim_crm_opportunity.dim_crm_opportunity_id, 
      CASE
       WHEN sheetload_map_ramp_deal.dim_crm_opportunity_id IS NOT NULL THEN sheetload_map_ramp_deal."Overwrite_SSP_ID" 
       WHEN zuora_legacy_ramps.opportunity_id IS NOT NULL THEN zuora_legacy_ramps.myb_opportunity_id
       WHEN zuora_ramps.opportunity_id IS NOT NULL THEN zuora_ramps.myb_opportunity_id
        WHEN ramp_deals.dim_crm_opportunity_id IS NOT NULL THEN ramp_deals.ssp_id     
      END AS ramp_ssp_id_init,
      CASE WHEN ramp_ssp_id_init <> 'Not a ramp' THEN ramp_ssp_id_init
      ELSE LEFT(zuora_ramps.opportunity_id, 15) END AS ramp_ssp_id,
      zuora_legacy_ramps.opportunity_id as zuora_legacy_opp_id,
      zuora_ramps.opportunity_id as zuora_opp_id,
      sheetload_map_ramp_deal.dim_crm_opportunity_id as sheetload_opp_id,
      ramp_deals.dim_crm_opportunity_id as sf_ramp_deal_opp_id
    FROM dim_crm_opportunity	        
    LEFT JOIN sheetload_map_ramp_deal       
     ON sheetload_map_ramp_deal.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id 
   LEFT JOIN ramp_deals          
     ON ramp_deals.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
   LEFT JOIN zuora_legacy_ramps
     ON zuora_legacy_ramps.opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    LEFT JOIN zuora_ramps
     ON zuora_ramps.opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    WHERE ramp_ssp_id IS NOT NULL 


--Getting Subscription information
), subscriptions_with_ssp_id AS (

    SELECT 
      ramp_deals_ssp_id_multiyear_linkage.ramp_ssp_id,
      dim_subscription.*				
    FROM dim_subscription			
    LEFT JOIN ramp_deals_ssp_id_multiyear_linkage				
    ON dim_subscription.dim_crm_opportunity_id = ramp_deals_ssp_id_multiyear_linkage.dim_crm_opportunity_id	

    
--Getting Last term version of the subscription         
), dim_subscription_latest_version AS (

    SELECT 
      ROW_NUMBER() OVER (PARTITION BY subscription_name, term_end_date ORDER BY ramp_ssp_id, subscription_version DESC) AS last_term_version,
      subscriptions_with_ssp_id.*       
    FROM subscriptions_with_ssp_id        
    Where subscription_status != 'Cancelled'        
    QUALIFY last_term_version = 1   


), dim_subscription_cancelled AS (  

    SELECT DISTINCT 
      subscription_name, 
      term_start_date 
    FROM dim_subscription	     
    Where subscription_status = 'Cancelled' 


---Subscriptions base
), dim_subscription_base AS (     

    SELECT 
      dim_subscription_latest_version.*
    FROM dim_subscription_latest_version        
    LEFT JOIN dim_subscription_cancelled        
      ON dim_subscription_latest_version.subscription_name = dim_subscription_cancelled.subscription_name       
      AND dim_subscription_latest_version.term_start_date >= dim_subscription_cancelled.term_start_date        
    WHERE dim_subscription_cancelled.subscription_name IS NULL  
    AND 
       --data quality, last version is expired with no ARR in mart_arr. Should filter it out completely.
       dim_subscription_id NOT IN ('2c92a0ff5e1dcf14015e3bb595f14eef','2c92a0ff5e1dcf14015e3c191d4f7689','2c92a007644967bc01645d54e7df49a8', '2c92a007644967bc01645d54e9b54a4b', '2c92a0ff5e1dcf1a015e3bf7a32475a5')
       --test subscription
       AND dim_subscription_latest_version.subscription_name != 'Test- New Subscription'
       --data quality, last term not entered with same pattern, sub_name = A-S00022101
       AND dim_subscription_id != '2c92a00f7579c362017588a2de19174a'
       --term dates do not align to the subscription term dates, sub_name = A-S00038937
       AND dim_subscription_id != '2c92a01177472c5201774af57f834a43'
       --data quality, last term not entered with same pattern that fits ATR logic. Edge cases that needs to be filtered out to get to the last term version that should count for this subscription.
       --sub_name = A-S00011774
       AND dim_subscription_id NOT IN ('8a1298657dd7f81d017dde1bd9c03fa8','8a128b317dd7e89a017ddd38a74d3037','8a128b317dd7e89a017ddd38a6052ff0',
                                       '8a128b317dc30baa017dc41e5b0932e9','8a128b317dc30baa017dc41e59dd32be','8a128b317dc30baa017dc41e58b43295',
                                       '2c92a0fd7cc1ab13017cc843195f62fb','2c92a0fd7cc1ab13017cc843186f62da','2c92a0fd7cc1ab13017cc843178162b6',
                                       '2c92a0fd7cc1ab13017cc843164d6292')


--Calculating min and max term dates for all ramps
), ramp_min_max_dates AS (

    SELECT 
      ramp_ssp_id, 
      MIN(term_start_date) AS min_term_start_date,  
      MAX(term_end_date) AS max_term_end_date  
    FROM dim_subscription_base      
    WHERE ramp_ssp_id IS NOT NULL       
    GROUP BY 1 HAVING COUNT(*) > 1  


----Calculating ATR start term and End term dates from Subscripion base
), subscriptions_for_all AS (    

    SELECT 
      dim_subscription_base.*, 
      CASE WHEN min_term_start_date IS NOT NULL THEN min_term_start_date 
      ELSE term_start_date 
      END AS ATR_term_start_date,       
      CASE WHEN max_term_end_date IS NOT NULL THEN max_term_end_date 
      ELSE term_end_date END AS ATR_term_end_date       
    FROM dim_subscription_base        
    LEFT JOIN ramp_min_max_dates       
      ON dim_subscription_base.ramp_ssp_id = ramp_min_max_dates.ramp_ssp_id        
    WHERE dim_subscription_base.ramp_ssp_id IS NULL
     OR (dim_subscription_base.ramp_ssp_id IS NOT NULL 
     AND max_term_end_date = term_end_date)  
   

  --ARR from charges and other columns as needed  
), subscription_charges AS (

    SELECT 
       subscriptions_for_all.dim_subscription_id,
      fct_charge.dim_charge_id,
      dim_crm_account.dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name,
      dim_product_detail_id,
      subscriptions_for_all.dim_crm_opportunity_id,
      fct_charge.dim_billing_account_id,
      dim_crm_user.crm_user_sales_segment,
      dim_crm_user.crm_user_geo,
      dim_crm_user.crm_user_region,
      dim_crm_user.crm_user_area,
      dim_crm_user.dim_crm_user_id,
      dim_crm_user.user_name,
      subscriptions_for_all.ATR_term_start_date,
      subscriptions_for_all.ATR_term_end_date,
      subscriptions_for_all.dim_crm_account_id, 
      subscriptions_for_all.subscription_name,
      quantity, 
      ARR,
      zuora_renewal_subscription_name AS renewal_subscription_name 
    FROM subscriptions_for_all    
    LEFT JOIN fct_charge   
      ON subscriptions_for_all.dim_subscription_id = fct_charge.dim_subscription_id        
      AND subscriptions_for_all.term_end_date = TO_VARCHAR(TO_DATE(TO_CHAR(effective_end_date_id),'yyyymmdd'), 'YYYY-MM-DD')   
      AND fct_charge.effective_start_date_id != fct_charge.effective_end_date_id            
    INNER JOIN dim_billing_account
      ON fct_charge.dim_billing_account_id = dim_billing_account.dim_billing_account_id
    LEFT JOIN dim_charge  
      ON dim_charge.dim_charge_id = fct_charge.dim_charge_id 
    LEFT JOIN dim_crm_account
      ON dim_crm_account.dim_crm_account_id = dim_billing_account.dim_crm_account_id
    LEFT JOIN dim_crm_user
      ON dim_crm_account.dim_crm_user_id = dim_crm_user.dim_crm_user_id
    WHERE fct_charge.dim_product_detail_id IS NOT NULL  
      AND dim_crm_account.is_jihu_account != 'TRUE'
      AND dim_charge.is_included_in_arr_calc = 'TRUE'

    
--Final ATR Calculation for all Quarters 
), final AS ( 

    SELECT DISTINCT
      dim_date.fiscal_quarter_name_fy, 
      dim_charge_id,
      dim_crm_account_id, 
      dim_crm_opportunity_id,
      dim_subscription_id, 
      subscription_name,
      renewal_subscription_name,
      dim_billing_account_id,
      dim_product_detail_id,
      dim_parent_crm_account_id,
      parent_crm_account_name,
      ATR_term_start_date,
      ATR_term_end_date,
      dim_crm_user_id,
      user_name,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      SUM(ARR) as ARR, 
      Quantity 
    FROM subscription_charges 
    LEFT JOIN dim_date
     ON subscription_charges.ATR_term_end_date = dim_date.date_day 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,21
)

{{ dbt_audit(
cte_ref="final",
created_by="@snalamaru",
updated_by="@snalamaru",
created_date="2024-04-01",
updated_date="2024-04-09"
) }}


