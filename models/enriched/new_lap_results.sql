SELECT 
    np.id as participation_id, 
    lap, 
    lap_time, 
    c_flag, 
    time_start, 
    time_end, 
    position_start, 
    position_end
FROM source.json_lap_results jlr 
left join {{ ref("new_drivers") }} nd on nd.index = jlr.driver_index
left join {{ ref("new_participations") }} np on np.driver_id = nd.id