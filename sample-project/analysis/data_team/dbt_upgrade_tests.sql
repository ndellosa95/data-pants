{%- set production_targets = production_targets() -%}

{% if execute and target.name not in production_targets %}


  {#
Example
tests:
  - name: compare_dim_date
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: dim_date
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at
      - current_date_actual
      - current_fiscal_year
      - CURRENT_FIRST_DAY_OF_MONTH
      - CURRENT_FIRST_DAY_OF_FISCAL_YEAR
      - CURRENT_FISCAL_QUARTER_NAME_FY

#}

  {% set tests_yml -%}


target_prefix: "DBT_1_6_UPGRADE_"

tests:
  - name: compare_dim_date
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: dim_date
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at
      - current_date_actual
      - current_fiscal_year
      - CURRENT_FIRST_DAY_OF_MONTH
      - CURRENT_FIRST_DAY_OF_FISCAL_YEAR
      - CURRENT_FISCAL_QUARTER_NAME_FY

  - name: compare_mart_product_usage_paid_user_metrics_monthly
    test_type: compare_relations
    source_db: prod
    source_schema: common_mart_product
    source_name: mart_product_usage_paid_user_metrics_monthly
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_fct_event
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: fct_event
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_mart_crm_opportunity_stamped_hierarchy_hist
    test_type: compare_relations
    source_db: prod
    source_schema: restricted_safe_common_mart_sales
    source_name: mart_crm_opportunity_stamped_hierarchy_hist
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at
      - days_since_last_activity

  - name: compare_mart_arr
    test_type: compare_relations
    source_db: prod
    source_schema: restricted_safe_common_mart_sales
    source_name: mart_arr
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_employee_directory_analysis
    test_type: compare_relations
    source_db: prod
    source_schema: legacy
    source_name: employee_directory_analysis
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_mart_charge
    test_type: compare_relations
    source_db: prod
    source_schema: restricted_safe_common_mart_sales
    source_name: mart_charge
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_mart_retention_parent_account
    test_type: compare_relations
    source_db: prod
    source_schema: restricted_safe_common_mart_sales
    source_name: mart_retention_parent_account
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_fct_ping_instance_metric_monthly
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: fct_ping_instance_metric_monthly
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_mart_available_to_renew
    test_type: compare_relations
    source_db: prod
    source_schema: restricted_safe_common_mart_finance
    source_name: mart_available_to_renew
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_rpt_event_xmau_metric_monthly
    test_type: compare_relations
    source_db: prod
    source_schema: common_mart_product
    source_name: rpt_event_xmau_metric_monthly
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_dim_ping_instance
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: dim_ping_instance
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at
      - IP_ADDRESS_HASH
      - IS_LAST_PING_OF_MONTH
      - IS_LAST_PING_OF_WEEK

  - name: compare_mart_ping_instance_metric
    test_type: compare_relations
    source_db: prod
    source_schema: common_mart
    source_name: mart_ping_instance_metric
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at
      - IS_LAST_PING_OF_MONTH
      - IS_LAST_PING_OF_WEEK

  - name: compare_dim_namespace
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: dim_namespace
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_dim_user
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: dim_user
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_gitlab_dotcom_members
    test_type: compare_relations
    source_db: prod
    source_schema: legacy
    source_name: gitlab_dotcom_members
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_snowplow_unnested_events_all
    test_type: compare_relations
    source_db: prod
    source_schema: legacy
    source_name: snowplow_unnested_events_all
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_dim_crm_account
    test_type: compare_relations
    source_db: prod
    source_schema: restricted_safe_common
    source_name: dim_crm_account
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at
      - LAST_MODIFIED_DATE
      - LAST_MODIFIED_BY_NAME

  - name: compare_dim_crm_user
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: dim_crm_user
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_gitlab_dotcom_projects_xf
    test_type: compare_relations
    source_db: prod
    source_schema: legacy
    source_name: gitlab_dotcom_projects_xf
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at
      - project_updated_at
      - last_activity_at
      - last_repository_updated_at


  {%- endset %}

  {%- set tests_dict = fromyaml(tests_yml) -%}

  {%- set tests = tests_dict.tests | list -%}

  {%- if tests | length < 1 %}
    {% do exceptions.raise_compiler_error('There are no tests configured.')%}
  {% endif -%}



  {%- for test in tests -%}

    {% set table_db = test.source_db | upper %}
    {% set target_db = tests_dict.target_prefix + table_db | upper %}
    {% set table_schema = test.source_schema | upper %}
    {% set table_name = test.source_name | upper %}
    {%- set old_etl_relation=adapter.get_relation(table_db, table_schema, table_name ) -%}
    {%- set new_etl_relation=adapter.get_relation(target_db, table_schema, table_name ) -%}

{{"-- "}}{{ old_etl_relation }}
{{ audit_helper.compare_relations(
    old_etl_relation,
    new_etl_relation,
    exclude_columns= test.exclude_columns
) | trim | replace(' 
    
      , 
     
   
    ', ',')  }}
;

  {%- endfor -%}

{% endif %}