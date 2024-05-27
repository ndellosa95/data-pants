{% macro ref(model_name) %}

  {% set relation = builtins.ref(model_name) %}

  {% if is_table_sampled(model_name) %}
    {% set new_relation = get_sample_relation(relation) %}
  {% else %}
    {% set new_relation = relation %}
  {% endif %}
  
  {% do return(new_relation) %}

{% endmacro %}