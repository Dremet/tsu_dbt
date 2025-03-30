{{
    config(
        unique_key=['p_id']
    )
}}

SELECT 
    id as p_id,
    event_id as e_e_id,
    driver_id as d_d_id,
    vehicle_id as v_v_id
FROM {{ ref("new_participations") }}