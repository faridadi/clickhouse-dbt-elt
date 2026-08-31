{# ==============================================================================
   MACRO: GENERATE SCHEMA NAME (CUSTOM TARGETING)
   Tujuan: Mencegah dbt menambahkan prefix 'default_' pada nama skema di Production.
   Digunakan oleh: dbt Core (secara otomatis saat melakukan materialisasi tabel/view).
============================================================================== #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    
    {%- if target.name == 'prod' -%}
        {# Jika jalan di Airflow Production: Langsung pakai nama asli (bronze_lion) #}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
        
    {%- else -%}
        {# Jika jalan di Laptop (dev): Tambahkan target schema di depannya (contoh: dev_satria_base_bronze_lion) #}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ default_schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
