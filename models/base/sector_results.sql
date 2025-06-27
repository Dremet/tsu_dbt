{{
    config(
        unique_key=['p_p_id', 's_lap', 's_number']
    )
}}


-- models/sector_results.sql
with all_sectors as (

    -- Keep only checkpoints that mark sector‑ends.
    -- A checkpoint with cp_index = 0 belongs to the *previous* lap.
    select
        p_p_id,
        case
            when cp_index = 0            then cp_lap - 1   -- finish line → previous lap
            else                              cp_lap
        end                              as s_lap,
        cp_time,
        cp_lap_c_flag,

        -- cumulative time of the previous sector‑end (across the whole stint)
        lag(cp_time) over (
            partition by p_p_id
            order by      cp_time
        )                                as prev_cp_time
    from {{ ref('checkpoint_results') }}
    where cp_is_sector = true

), filtered as (

    -- Drop the initial “rolling‑start” row (it has s_lap = 0).
    select *
    from all_sectors
    where s_lap >= 1

), sector_times as (

    -- Pure sector time = Δ to previous sector‑end
    select
        p_p_id,
        s_lap,
        cp_time - prev_cp_time       as s_time,
        cp_lap_c_flag,
        cp_time                      as sector_end_time    -- for ordering
    from filtered
)

select
    p_p_id,
    s_lap,
    row_number() over (                 -- starts at 1, resets every lap
        partition by p_p_id, s_lap
        order by      sector_end_time
    )                                   as s_number,
    s_time,
    cp_lap_c_flag                       as s_lap_c_flag
from sector_times
order by p_p_id, s_lap, s_number
