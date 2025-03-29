SELECT 
    np.id as participation_id, 
    finish_time, 
    laps_completed, 
    last_checkpoint
FROM source.json_race_results jrr
left join {{ ref("new_drivers") }} nd on nd.index = jrr.driver_index
left join {{ ref("new_participations") }} np on np.driver_id = nd.id