{{
    config(
        unique_key=['p_p_id','lr_lap']
    )
}}

SELECT 
    participation_id as p_p_id,
    lap as lr_lap, 
    lap_time as lr_time, 
    c_flag as lr_c_flag, 
    time_start as lr_time_start, 
    time_end as lr_time_end, 
    position_start as lr_position_start, 
    position_end as lr_position_end
FROM {{ ref("new_lap_results") }}