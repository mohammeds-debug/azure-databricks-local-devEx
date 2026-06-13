{% if target.name == 'local' %}

select * from read_parquet('{{ var("cache_dir") }}/raw_events/*.parquet')

{% else %}

select * from {{ source('bronze_source', 'raw_events') }}

{% endif %}
