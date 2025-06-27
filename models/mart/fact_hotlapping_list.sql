{{
    config(
        materialized='table',
        unique_key=['e_id']
    )
}}

with base as (
    select 
        e.e_id,
        e.e_timestamp,
        t.tr_id,
        t.tr_name,
        v.v_name,
        p.p_id,
        h.h_id as h_h_id
    from {{ ref('events') }} e
    left join {{ ref('tracks') }} t on e.tr_tr_id = t.tr_id
    left join {{ ref('participations') }} p on e.e_id = p.e_e_id
    left join {{ ref('vehicles') }} v on p.v_v_id = v.v_id
    inner join {{ ref('hotlapping') }} h on e.e_id = h.e_e_id
    where e_server = 'hotlapping' and e_is_hotlapping = true 
),

aggregated as (
    select 
        h_h_id,
        tr_name,
        min(e_timestamp) as event_start,
        array_agg(distinct v_name) as cars_used,
        count(p_id) as number_of_race_results
    from base
    group by h_h_id, tr_name
)

select *
from aggregated
order by h_h_id
