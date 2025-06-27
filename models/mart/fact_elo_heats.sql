-- elo from tripleheats sorted by position (highest elo)

{{
    config(
        materialized='table'
    )
}}

with all_event_elos as (
    select 
        d.d_id as d_d_id,
        d.d_name as d_name,
        p.e_e_id,
        ee.e_value as eh_current_elo,
        ee.e_delta as eh_elo_delta,
        sum(ee.e_delta) over (
            partition by d.d_id
            order by e.e_timestamp desc
            rows between 5 preceding and current row
        ) as eh_elo_delta_6,
        row_number() over (
            partition by d.d_id
            order by     e.e_timestamp desc
        ) as rn,
        count(p.p_id) over (partition by d.d_id) as eh_participations
    from {{ ref('drivers') }} d
    inner join {{ ref('participations') }} p on d.d_id = p.d_d_id
    inner join {{ ref('elo_heats') }} ee on p.p_id = ee.p_p_id
    inner join {{ ref('events') }} e on p.e_e_id = e.e_id
)
select 
    *
from all_event_elos
where rn = 1 and eh_participations >= 3