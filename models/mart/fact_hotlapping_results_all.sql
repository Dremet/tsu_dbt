-- all hotlapping results
{{
    config(
        materialized='table'
    )
}}

with aggregated as (
    select 
        h_id,
        e.e_timestamp,
        p.d_d_id,
        p.v_v_id,
        lr.lr_time,
        -- ordered list of sector times
        array_agg(
            round(s_time::numeric, 4)
            order by s_number asc
        )                      as s_times,
        count(*)               as s_count
    from {{ ref('hotlapping') }} h
    left join {{ ref('events') }} e on e.e_id = h.e_e_id
    left join {{ ref('participations') }} p on e.e_id = p.e_e_id
    left join {{ ref('lap_results') }} lr on p.p_id = lr.p_p_id
    left join {{ ref("sector_results")}} s on s.p_p_id = p.p_id and s.s_lap = lr.lr_lap
    group by h_id, e.e_timestamp, p.d_d_id, p.v_v_id, lr.lr_time
    having count(*) <= 10 
)
select
    h_id,
    e_timestamp,
    a.d_d_id,
    d.d_name,
    d.d_steam_id,
    v.v_name,
    lr_time as h_lap_time,
    -- consistency flag     
    s_times,
    s_count
from aggregated a
left join {{ ref('vehicles') }} v on a.v_v_id = v.v_id
left join {{ ref('drivers') }} d on a.d_d_id = d.d_id
where lr_time is not null