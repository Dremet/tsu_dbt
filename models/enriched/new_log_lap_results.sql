SELECT
    np.id as participation_id,
    time_start,
    time_end,
    lap,
    tire_compound_start,
    tire_wear_start,
    tire_wear_end,
    tire_perc_start,
    tire_perc_end,
    fuel_used_start,
    fuel_used_end,
    fuel_perc_start,
    fuel_perc_end,
    hit_points_start,
    hit_points_end,
    is_inlap,
    is_outlap,
    tire_perc_avg,
    fuel_perc_avg,
    hit_points_avg,
    fuel_used,
    tire_used,
    lap_time,
    position_start,
    position_end
FROM source.log_main lm 
left join {{ ref("new_drivers") }} nd on nd.index = lm.driver_id
left join {{ ref("new_participations") }} np on np.driver_id = nd.id