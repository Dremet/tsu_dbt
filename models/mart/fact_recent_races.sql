-- last 10 races sorted by timestamp desc

-- timestamp
-- track (laps)
-- participants
-- cars
-- winner

{{
    config(
        materialized='table',
        unique_key=['e_id']
    )
}}

WITH race_stats AS (
    SELECT 
        e.e_id,
        e.e_timestamp as timestamp,
        t.tr_name as track_name,
        COUNT(DISTINCT p.d_d_id) AS participants,
        (
            SELECT STRING_AGG(DISTINCT v.v_name, ', ' ORDER BY v.v_name)
            FROM {{ ref("participations") }} p2
            LEFT JOIN {{ ref("vehicles") }} v ON p2.v_v_id = v.v_id
            WHERE p2.e_e_id = e.e_id
        ) as cars
    FROM {{ ref("events") }} e
    LEFT JOIN {{ ref("tracks") }} t ON e.tr_tr_id = t.tr_id
    LEFT JOIN {{ ref("participations") }} p ON e.e_id = p.e_e_id
    WHERE e.e_server in ('events','heats')
    GROUP BY e.e_id, e.e_timestamp, t.tr_name
),

race_results_with_rank AS (
    SELECT 
        e.e_id,
        d.d_name as winner,
        rr.rr_laps_completed as laps,
        ROW_NUMBER() OVER (PARTITION BY e.e_id ORDER BY rr.rr_laps_completed DESC, rr_last_checkpoint ASC, rr_finish_time ASC) as finish_rank
    FROM {{ ref("events") }} e
    LEFT JOIN {{ ref("participations") }} p ON e.e_id = p.e_e_id
    LEFT JOIN {{ ref("race_results") }} rr ON p.p_id = rr.p_p_id
    LEFT JOIN {{ ref("drivers") }} d ON p.d_d_id = d.d_id
    WHERE rr.rr_finish_time IS NOT NULL
),

winners AS (
    SELECT 
        e_id,
        winner,
        laps
    FROM race_results_with_rank
    WHERE finish_rank = 1
)

SELECT 
    rs.e_id,
    rs.timestamp,
    rs.track_name || ' (' || w.laps || ' laps)' as track,
    rs.participants,
    rs.cars,
    w.winner
FROM race_stats rs
LEFT JOIN winners w ON rs.e_id = w.e_id
ORDER BY rs.timestamp DESC
LIMIT 10