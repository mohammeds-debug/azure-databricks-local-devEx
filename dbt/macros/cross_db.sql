{#
  Thin adapters for operations where DuckDB (local) and SparkSQL (Databricks prod)
  have diverging syntax. All silver/gold SQL should use these macros instead of
  calling engine-specific functions directly.
#}


{# Extract a string value from a JSON column.
   local (DuckDB): json_extract_string(col, path)
   prod  (Spark):  get_json_object(col, path)        #}
{% macro json_str(col, path) %}
  {%- if target.name == 'local' -%}
    json_extract_string({{ col }}, '{{ path }}')
  {%- else -%}
    get_json_object({{ col }}, '{{ path }}')
  {%- endif -%}
{% endmacro %}


{# COUNT(*) restricted to rows matching a condition.
   local (DuckDB): count(*) filter (where <cond>)
   prod  (Spark):  count(case when <cond> then 1 end)  #}
{% macro count_if(condition) %}
  {%- if target.name == 'local' -%}
    count(*) filter (where {{ condition }})
  {%- else -%}
    count(case when {{ condition }} then 1 end)
  {%- endif -%}
{% endmacro %}


{# AVG(<expr>) restricted to rows matching a condition.
   local (DuckDB): avg(<expr>) filter (where <cond>)
   prod  (Spark):  avg(case when <cond> then <expr> end)  #}
{% macro avg_if(expr, condition) %}
  {%- if target.name == 'local' -%}
    avg({{ expr }}) filter (where {{ condition }})
  {%- else -%}
    avg(case when {{ condition }} then {{ expr }} end)
  {%- endif -%}
{% endmacro %}
