{% macro scd2_join_on_date(dim_alias, fact_date_col) -%}
  {{ dim_alias }}.valid_from <= {{ fact_date_col }}
  and ({{ dim_alias }}.valid_to is null or {{ fact_date_col }} < {{ dim_alias }}.valid_to)
{%- endmacro %}