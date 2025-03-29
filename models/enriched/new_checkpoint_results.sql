SELECT 
    np.id as participation_id,
    lap, 
    lap_c_flag, 
    cp, 
    is_sector, 
    cp_time, 
    "position"
FROM source.json_checkpoint_results jcr 
left join {{ ref("new_drivers") }} nd on nd.index = jcr.driver_index
left join {{ ref("new_participations") }} np on np.driver_id = nd.id