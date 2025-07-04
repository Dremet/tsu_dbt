{{
    config(
        unique_key=['v_id']
    )
}}

SELECT 
    id as v_id,
    vehicle_name as v_name, 
    vehicle_guid as v_guid
FROM {{ ref("new_vehicles") }}
