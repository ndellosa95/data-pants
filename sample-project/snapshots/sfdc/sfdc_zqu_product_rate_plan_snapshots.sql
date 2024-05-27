{% snapshot sfdc_zqu_product_rate_plan_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='systemmodstamp',
        )
    }}

    SELECT *
    FROM {{ source('salesforce', 'zqu_product_rate_plan') }}

{% endsnapshot %}
