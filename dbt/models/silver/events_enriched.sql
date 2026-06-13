with events as (
    select * from {{ ref('stg_raw_events') }}
),

tracking as (
    select * from {{ ref('stg_event_tracking') }}
),

enriched as (
    select
        -- event identity
        e.event_id,
        e.event_timestamp,
        e.event_type,
        e.status,
        e.duration_seconds,

        -- assignee (first entry in JSON array -- uses cross_db macro for DuckDB/Spark parity)
        {{ json_str('e.assignees', '$[0].email') }} as assignee_email,
        {{ json_str('e.assignees', '$[0].name') }}  as assignee_name,

        -- source / target identifiers
        e.source_id,
        e.target_id,

        -- processing pipeline status
        t.status            as processing_status,
        t.retry_count,
        t.updated_at        as tracking_updated_at,

        -- ingestion metadata
        e._ingested_at,
        e._ingestion_run

    from events e
    left join tracking t on e.event_id = t.event_id
)

select * from enriched
