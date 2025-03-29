{{
    config(
        unique_key=['e_e_id','c_name']
    )
}}

SELECT 
    event_id as e_e_id,
    name as c_name,
    max_wear as c_max_wear,
    max_performance as c_max_performance
FROM {{ ref("new_compounds") }}