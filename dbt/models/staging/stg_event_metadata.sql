{% if target.name == 'local' %}

select * from read_parquet('{{ var("cache_dir") }}/event_metadata/*.parquet')

{% else %}

select * from {{ source('bronze_source', 'event_metadata') }}

{% endif %}
