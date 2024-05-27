{% docs unpack_unstructured_event %}
This macro unpacks the unstructured snowplow events. It takes a list of field names, the pattern to match for the name of the event, and the prefix the new fields should use.
{% enddocs %}

{% docs clean_url %}
This macro will take a page_url_path string and use regex to strip out information which is not required for analysis. The regex explanation can be found here: https://regex101.com/r/nSdGr7/1
{% enddocs %}

{% docs snowplow_schema_field_aliasing %}
A Snowplow event can provide additional fields through the `context`. These are defined by the developer through schemas, which can be incremented to indicate changes to the columns, data types, etc. To include these columns in each Snowplow event, we can use this macro to expand the context field based on the schema and columns we are looking for in the context.

This macro has the following required arguments:
- `schema`: Defined by the schema documentation and found within the Snowplow context payload. We prefer to use the wildcard (`%`) notation at the end of the schema instead of limiting to the current schema number in case of future schemas.
- `context_name`: Chosen by the analyst in coordination with their business stakeholder to give a "friendly" or common name to the context throughout the Enterprise Dimensional Model. This name will be applied to the `[context_name]_context`, `[context_name]_context_schema`, and `has_[context_name]_context` columns.

The third parameter has some required and some non-required fields:
- `field_alias_datatype_list`: A dictionary of key value pairs that defines the logic for a given field in the Snowplow context. Default values can be applied in the macro, so only the `field` column is required. However, the user can choose to define a `formula`, `data_type`, and/or `alias` for each field in the context schema.
    - REQUIRED:
        - `field`: The name of the field as it is written in the Snowplow schema documentation and context field.
    - OPTIONAL:
        - `formula`: Any SQL logic that the analyst wants to apply to the column. If this is blank, standard logic to flatten the column from context column will be applied.
        - `data_type`: The default data type for any column will be VARCHAR, but the analyst can choose to cast the column as any data type using this key-value pair.
        - `alias`: If the analyst want to rename the field, they can fill in the alias column, otherwise the macro will use the `field` column as the alias.

Examples:

```text
snowplow_schema_field_aliasing(
schema='iglu:com.gitlab/gitlab_experiment/jsonschema/%',
context_name='gitlab_experiment',
field_alias_datatype_list=[
  {'field':'experiment'},
  {'field':'migration_keys', 'formula':"ARRAY_TO_STRING(context_data['migration_keys']::VARIANT, ', ')" , 'data_type':'text', 'alias':'experiment_migration_keys'}
   ]
  )
```

In this example, `experiment`, would have all default settings applied (no additional formula logic, cast as VARCHAR, aliased as experiment). The other column, `migration_keys` would have additional logic applied to convert it from an array to a string, it would be cast as text (done for demonstration purposes in this example since it is synonymous with VARCHAR), and it is renamed as `experiment_migration_keys`.

{% enddocs %}