with base as (
    select * from {{ ref('events_enriched') }}
    where assignee_email is not null
),

stats as (
    select
        cast(event_timestamp as date)                                   as event_date,
        assignee_email,
        assignee_name,

        count(*)                                                        as total_events,
        {{ count_if("status = 'completed'") }}                          as completed_events,
        {{ count_if("status = 'missed'") }}                             as missed_events,
        {{ count_if("status = 'cancelled'") }}                          as cancelled_events,

        round(
            100.0 * {{ count_if("status = 'completed'") }}
            / nullif(count(*), 0),
        1)                                                              as completion_rate_pct,

        round({{ avg_if('duration_seconds', "status = 'completed' and duration_seconds > 0") }}, 1)
                                                                        as avg_duration_seconds,

        max(duration_seconds)                                           as max_duration_seconds,

        {{ count_if("event_type = 'inbound'") }}                        as inbound_events,
        {{ count_if("event_type = 'outbound'") }}                       as outbound_events

    from base
    group by 1, 2, 3
)

select * from stats
order by event_date desc, total_events desc
