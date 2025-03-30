{{
    config(
        unique_key=['p_p_id']
    )
}}

select
    p_p_id,
    e_value,
    e_delta
-- this is filled via update_elo.py script in main directory
from enriched.new_event_elos