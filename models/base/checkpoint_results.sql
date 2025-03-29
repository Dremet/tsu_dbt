{{
    config(
        unique_key=['p_p_id', 'cp_lap', 'cp_index']
    )
}}

SELECT 
    participation_id as p_p_id,
    lap as cp_lap, 
    cp as cp_index, 
    cp_time as cp_time,
    lap_c_flag as cp_lap_c_flag, 
    is_sector as cp_is_sector,  
    "position" as cp_position 
FROM {{ ref("new_checkpoint_results") }}