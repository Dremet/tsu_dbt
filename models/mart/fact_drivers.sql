-- mainly for driver page
{{
    config(
        materialized='table'
    )
}}

select 
    d.*,
    ee.ee_current_elo as d_current_event_elo,
    ee.ee_elo_delta as d_event_elo_delta--,
    --eh.e
from {{ ref("drivers")}} d
left join {{ ref("fact_elo_events") }} ee on d.d_id = ee.d_d_id
left join {{ ref("fact_elo_heats") }} eh on d.d_id = ee.d_d_id