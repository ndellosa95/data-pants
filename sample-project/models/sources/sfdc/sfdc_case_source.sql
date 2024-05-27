WITH source AS (

  SELECT *
  FROM {{ source('salesforce', 'case') }}

),

renamed AS (
  SELECT
    id AS case_id,
    account_owner__c AS account_owner,
    accountid AS account_id,
    businesshoursid AS business_hours_id,
    casenumber AS case_number,
    closeddate AS closed_date,
    contactemail AS contact_email,
    contactfax AS contact_fax,
    contactid AS contact_id,
    contactmobile AS contact_mobile,
    contactphone AS contact_phone,
    description,
    from_chatter_feed_id__c AS from_chatter_feed_id,
    isclosed AS is_closed,
    isclosedoncreate AS is_closed_on_create,
    isescalated AS is_escalated,
    origin,
    ownerid AS owner_id,
    priority,
    reason,
    recordtypeid AS record_type_id,
    resolution_action__c AS resolution_action,
    sourceid AS source_id,
    status,
    subject,
    suppliedcompany AS supplied_company,
    suppliedemail AS supplied_email,
    suppliedname AS supplied_name,
    suppliedphone AS supplied_phone,
    type AS case_type,
    opportunity__c AS opportunity_id,
    Close_Case_Spam__c AS spam_checkbox, 
    context__c AS context,
    feedback__c AS feedback,
    Date_Time_First_Responded__c AS first_responded_date,
    Time_to_First_Response__c AS time_to_first_response, 
    Notes__c AS next_steps,
    Next_Steps_Date__c AS next_steps_date,
    
    -- metadata
    createdbyid AS created_by_id,
    createddate AS created_date,
    isdeleted AS is_deleted,
    lastmodifiedbyid AS last_modified_by_id,
    lastmodifieddate AS last_modified_date,
    systemmodstamp,
    CONVERT_TIMEZONE(
      'America/Los_Angeles', CURRENT_TIMESTAMP()
    ) AS _last_dbt_run
  FROM source

)

SELECT *
FROM renamed
