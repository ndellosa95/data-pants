{% snapshot sfdc_zqu_zproduct_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='systemmodstamp',
        )
    }}

    SELECT *
    FROM {{ source('salesforce', 'zqu_zproduct') }}

{% endsnapshot %}
