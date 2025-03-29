{{
    config(
        unique_key=['p_p_id','ll_lap']
    )
}}

SELECT 
    participation_id as p_p_id,
    lap as ll_lap,
    lap_time as ll_time,
    time_start as ll_time_start,
    time_end as ll_time_end,
    tire_compound_start as ll_tire_compound_start,
    tire_wear_start as ll_tire_wear_start,
    tire_wear_end as ll_tire_wear_end,
    tire_perc_start as ll_tire_perc_start,
    tire_perc_end as ll_tire_perc_end,
    fuel_used_start as ll_fuel_used_start,
    fuel_used_end as ll_fuel_used_end,
    fuel_perc_start as ll_fuel_perc_start,
    fuel_perc_end as ll_fuel_perc_end,
    hit_points_start as ll_hit_points_start,
    hit_points_end as ll_hit_points_end,
    is_inlap as ll_is_inlap,
    is_outlap as ll_is_outlap,
    tire_perc_avg as ll_tire_perc_avg,
    fuel_perc_avg as ll_fuel_perc_avg,
    hit_points_avg as ll_hit_points_avg,
    fuel_used as ll_fuel_used,
    tire_used as ll_tire_used,
    position_start as ll_position_start,
    position_end as ll_position_end
FROM {{ ref("new_log_lap_results") }}