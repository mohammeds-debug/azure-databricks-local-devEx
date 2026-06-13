{% if target.name == 'local' %}

select * from read_parquet('{{ var("cache_dir") }}/event_tracking/*.parquet')

{% else %}

select * from {{ source('bronze_source', 'event_tracking') }}

{% endif %}
