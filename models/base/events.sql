{{
    config(
        unique_key=['e_id']
    )
}}

SELECT 
    id as e_id,
    track_id as t_t_id,
    utc_timestamp as e_timestamp, 
    host as e_host, 
    event_type as e_type,
    finished_state as e_finished_state, 
    max_laps as e_max_laps, 
    max_time_without_start_time as e_max_time_wo_start_time, 
    start_time as e_start_time, 
    hotlapping as e_is_hotlapping, 
    participants as e_number_participants,
    server as e_server
FROM {{ ref("new_event") }}