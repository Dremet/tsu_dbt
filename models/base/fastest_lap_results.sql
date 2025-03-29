{{
    config(
        unique_key=['p_p_id']
    )
}}

SELECT 
    participation_id as p_p_id,
    lap as fl_lap, 
    lap_time as fl_time, 
    c_flag as fl_c_flag, 
    "position" as fl_position
FROM {{ ref("new_fastest_lap_results") }}