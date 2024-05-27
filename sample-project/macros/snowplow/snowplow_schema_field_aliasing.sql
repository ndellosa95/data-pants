{% macro snowplow_schema_field_aliasing(schema, context_name, field_alias_datatype_list = {'field': None, 'formula': None, 'data_type': None, 'alias': None}) %}

  IFF(context_data_schema LIKE '{{schema}}', context_data, NULL)                              AS {{context_name}}_context,
  
  IFF(context_data_schema LIKE '{{schema}}', context_data_schema, NULL)                       AS {{context_name}}_context_schema,

  IFF(context_data_schema LIKE '{{schema}}', TRUE, FALSE)::BOOLEAN                            AS has_{{context_name}}_context,

{%- for context_col in field_alias_datatype_list %}

  {%- if context_col.get('field') -%}
    {% set field = context_col.get('field') %}
  {%- else -%}
    {% do exceptions.warn("Warning: You must enter at least one field for the snowplow_schema_field_aliasing to flatten column(s).") %}
  {%- endif -%}

  {%- if context_col.get('formula') -%}
    {% set formula = context_col.get('formula') %}
  {%- else -%}
    {% set formula = "context_data['"+field+"']" %}
  {%- endif -%}

  {%- if context_col.get('data_type')-%}
    {% set data_type = context_col.get('data_type')|upper %}
  {%- else -%}
    {% set data_type = "VARCHAR" %}
  {%- endif -%}

  {%- if context_col.get('alias') -%}
    {% set alias = context_col.get('alias') %}
  {%- else -%}
    {% set alias = context_col.get('field') %}
  {%- endif -%}

  IFF(context_data_schema LIKE '{{schema}}', {{formula}}::{{data_type}}, NULL)  AS {{alias}}
  {%- if not loop.last %}
    ,
  {% endif %}

{%- endfor -%}
{% endmacro %}