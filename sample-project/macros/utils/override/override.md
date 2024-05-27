{% docs ref %}
This macro will return a the normal relation unless one of the following conditions are met:
- The table is configured to be sampled, will return relation with the sample_suffix variable appended to the table name

{% enddocs %}

{% docs generate_schema_name %}

This is the GitLab overwrite for the dbt internal macro. 

This macro will write to custom schemas not on prod

Definitions:
    - custom_schema_name: schema provided via dbt_project.yml or model config
    - target.name: name of the target (dev for local development, prod for production, etc.)
    - target.schema: schema provided by the target defined in profiles.yml

This macro is hard to test, but here are some test cases and expected output.
(custom_schema_name, target.name, target.schema) = <output>

In all cases it will now write to the same schema. The database is what's 
different. See generate_database_name.sql

(legacy, prod, preparation) = legacy
(legacy, ci, preparationprod) = legacy
(legacy, dev, preparation) = legacy

(zuora, prod, preparation) = zuora
(zuora, ci, preparation) = zuora
(zuora, dev, preparation) = zuora

{% enddocs %}

{% docs generate_database_name %}
This macro will write to custom databases not on prod

Definitions:
    - custom_database_name: database provided via dbt_project.yml or model config
    - target.name: name of the target (dev for local development, prod for production, etc.)
    - target.database: database provided by the target defined in profiles.yml

Assumptions:
    - dbt users will have USERNAME_PROD, USERNAME_PREP DBs defined

This macro is hard to test, but here are some test cases and expected output.
(custom_database_name, target.name, target.database) = <output>


(prod, prod, prep) = prod
(prod, ci, prep) = prod
(prod, dev, tmurphy) = tmurphy_prod

(prep, prod, prep) = prep
(prep, ci, prep) = prep
(prep, dev, tmurphy) = tmurphy_prep

{% enddocs %}