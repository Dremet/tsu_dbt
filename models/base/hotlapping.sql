{{
    config(
        unique_key=['h_id','e_e_id']
    )
}}


with base as (
    select 
        e.e_id,
        e.e_timestamp,
        t.tr_id,
        row_number() over (order by e.e_timestamp) as row_num,
        lag(t.tr_id) over (order by e.e_timestamp) as prev_tr_id
    from {{ ref('events') }} e
    left join {{ ref('tracks') }} t on e.tr_tr_id = t.tr_id
    where e_server = 'hotlapping' and e_is_hotlapping = true 
),

change_points as (
    select *,
        case 
            when tr_id != prev_tr_id or prev_tr_id is null then 1
            else 0
        end as is_new_event
    from base
),

numbered as (
    select *,
        sum(is_new_event) over (order by row_num) as h_id
    from change_points
),

aggregated as (
    select 
        h_id,
        e_id as e_e_id
    from numbered
    group by h_id, e_e_id
)

select *
from aggregated
