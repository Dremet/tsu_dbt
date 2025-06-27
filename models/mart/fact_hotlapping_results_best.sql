-- Best hotlapping result by event and driver
{{
    config(
        materialized='table'
    )
}}

with ranked as (
    -- Rank all lap times per driver and event to identify their best lap
    select
        *,
        row_number() over (
            partition by h_id, d_d_id
            order by     h_lap_time
        ) as rn
    from {{ ref('fact_hotlapping_results_all') }}
),

aggregated as (
    -- Select only the best lap per driver per event
    select
        h_id,
        d_d_id,
        e_timestamp,
        d_name,
        d_steam_id,
        v_name,
        h_lap_time,
        min(h_lap_time) over (
            partition by h_id
        ) as h_best_lap_time,
        row_number() over (
            partition by h_id
            order by     h_lap_time
        ) as h_position,
        s_times,
        s_count
    from ranked r
    where rn = 1
),

consistency_check as (
    -- Count how many laps per driver/event are within 1% and 0.3% slower than their personal best
    select
        h_id,
        d_d_id,
        count(*) filter (
            where h_lap_time <= best_driver_lap * 1.01
        ) as laps_within_1pct,
        count(*) filter (
            where h_lap_time <= best_driver_lap * 1.003
        ) as laps_within_0_3pct
    from (
        select
            r.h_id,
            r.d_d_id,
            r.h_lap_time,
            min(r.h_lap_time) over (
                partition by r.h_id, r.d_d_id
            ) as best_driver_lap
        from {{ ref('fact_hotlapping_results_all') }} r
    ) laps
    group by h_id, d_d_id
),

final as (
    -- Join consistency info to best lap info
    select 
        a.*,
        a.h_lap_time - a.h_best_lap_time as h_diff_to_best_lap,
        c.laps_within_1pct >= 5 as h_is_consistent,         -- at least 5 laps within 1% (including best)
        c.laps_within_0_3pct >= 5 as h_is_very_consistent    -- at least 5 laps within 0.3%
    from aggregated a
    left join consistency_check c
        on a.h_id = c.h_id and a.d_d_id = c.d_d_id
)

select *
from final
order by h_id, h_position
