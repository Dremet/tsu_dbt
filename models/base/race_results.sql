{{
    config(
        unique_key=['p_p_id']
    )
}}

SELECT 
    participation_id as p_p_id,
    finish_time as rr_finish_time, 
    laps_completed as rr_laps_completed, 
    last_checkpoint as rr_last_checkpoint
FROM {{ ref("new_race_results") }}