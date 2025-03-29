SELECT 
    np.id as participation_id,
    lap, 
    lap_time, 
    c_flag, 
    "position"
FROM source.json_fastest_lap_results jflr 
left join {{ ref("new_drivers") }} nd on nd.index = jflr.driver_index
left join {{ ref("new_participations") }} np on np.driver_id = nd.id