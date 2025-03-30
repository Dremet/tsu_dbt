#!/usr/bin/env python3

import os
import sys
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

K_FACTOR = 20
D = 400  # Used to calculate the expected score in the Elo formula


def create_elo_table_if_not_exists(conn, table_fullname: str):
    """
    Creates an Elo table if it does not already exist.
    The table will have columns: p_p_id (primary key), e_value, and e_delta.
    """
    create_stmt = f"""
        CREATE TABLE IF NOT EXISTS {table_fullname} (
            p_p_id      TEXT PRIMARY KEY,
            e_value     DOUBLE PRECISION,
            e_delta     DOUBLE PRECISION
        );
    """
    conn.execute(text(create_stmt))


def get_last_elo_map(conn, read_table: str, server: str) -> dict:
    """
    Retrieves the latest Elo value for each driver from the specified base table.
    The query joins the participations and events base tables to fetch the Elo entry
    from the event with the highest e_timestamp for each driver.

    Returns a dictionary {driver_id: latest_elo_value}. Drivers with no entry will default to 1000.
    """
    sql = f"""
        WITH last_elo AS (
            SELECT
                p.d_d_id,
                e_elo.e_value,
                ev.e_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY p.d_d_id 
                    ORDER BY ev.e_timestamp DESC
                ) AS rn
            FROM {read_table} AS e_elo
            JOIN base.participations p 
                ON p.p_id = e_elo.p_p_id
            JOIN base.events ev
                ON ev.e_id = p.e_e_id
            WHERE ev.e_server = :server
        )
        SELECT d_d_id, e_value
        FROM last_elo
        WHERE rn = 1
    """
    df = pd.read_sql(text(sql), conn, params={"server": server})
    return df.set_index("d_d_id")["e_value"].to_dict()


def fetch_new_race_data(conn, server: str) -> pd.DataFrame:
    """
    Fetches new race results for the specified server type from the enriched schema.
    Joins the new_race_results, new_participations, and new_event tables.
    """
    sql = """
        SELECT 
            r.participation_id AS p_id,
            r.finish_time,
            r.laps_completed,
            r.last_checkpoint,
            p.event_id AS e_id,
            p.driver_id AS d_id,
            e.utc_timestamp,
            e.server AS e_server
        FROM enriched.new_race_results r
        JOIN enriched.new_participations p 
            ON p.id = r.participation_id
        JOIN enriched.new_event e 
            ON e.id = p.event_id
        WHERE e.server = :srv
    """
    return pd.read_sql(text(sql), conn, params={"srv": server})


def calc_expected_score(driver_elo: float, opponent_elo: float) -> float:
    """
    Calculates the expected score for a driver when facing a single opponent,
    based on the Elo formula.
    """
    return 1.0 / (1.0 + 10.0 ** ((opponent_elo - driver_elo) / D))


def run_elo_for_one_event(event_df: pd.DataFrame, last_elo_map: dict) -> pd.DataFrame:
    """
    Calculates new Elo values for all participants in a single event.
    Returns a DataFrame with columns: [p_id, new_elo, delta, d_id].
    Skips participants if there are no opponents.
    """
    sorted_df = event_df.sort_values(
        by=["laps_completed", "finish_time", "last_checkpoint"],
        ascending=[False, True, False],
    ).reset_index(drop=True)

    sorted_df["position"] = sorted_df.index + 1

    results = []
    for _, row in sorted_df.iterrows():
        participation_id = row["p_id"]
        driver_id = row["d_id"]
        position = row["position"]

        old_elo = last_elo_map.get(driver_id, 1000.0)
        opponents_df = sorted_df[sorted_df["d_id"] != driver_id]
        opponents_count = len(opponents_df)

        if opponents_count == 0:
            # Skip Elo calculation and storage if no opponents exist
            continue

        expected_score_sum = 0.0
        for _, opp_row in opponents_df.iterrows():
            opp_id = opp_row["d_id"]
            opp_elo = last_elo_map.get(opp_id, 1000.0)
            expected_score_sum += calc_expected_score(old_elo, opp_elo)

        overall_players = opponents_count + 1
        expected_score = expected_score_sum / (overall_players * opponents_count / 2.0)
        scoring = (overall_players - position) / (
            overall_players * opponents_count / 2.0
        )
        elo_change = K_FACTOR * opponents_count * (scoring - expected_score)
        new_elo = max(old_elo + elo_change, 100.0)
        delta = new_elo - old_elo

        results.append(
            {
                "p_id": participation_id,
                "new_elo": new_elo,
                "delta": delta,
                "d_id": driver_id,
            }
        )

    return pd.DataFrame(results)


def run_elo_calculation(db_url: str, table_name: str):
    """
    Main function to run the Elo calculation.

    Reads current Elo values from the base table and writes new Elo entries into the source table.
    The table_name parameter must be either 'events' or 'heats'.
    """
    if table_name not in ["events", "heats"]:
        raise ValueError("Only 'events' or 'heats' are allowed as table name argument.")

    # Define full table names (the schema is fixed)
    read_table = f"base.elo_{table_name}"
    write_table = (
        "enriched.new_event_elos"
        if table_name == "events"
        else "enriched.new_heat_elos"
    )

    engine = create_engine(db_url)

    with engine.begin() as conn:
        # Create both read and write tables if they do not exist
        create_elo_table_if_not_exists(conn, read_table)
        create_elo_table_if_not_exists(conn, write_table)

        # Retrieve current Elo values from the base table
        last_elo_map = get_last_elo_map(conn, read_table, table_name)

        # Fetch new race data for the specified server type
        df_all = fetch_new_race_data(conn, table_name)
        df_all = df_all.sort_values("utc_timestamp", ascending=True).reset_index(
            drop=True
        )

        # Process events in chronological order
        grouped = df_all.groupby("e_id", as_index=False)
        rows_to_insert = []
        for event_id, group_df in grouped:
            result_df = run_elo_for_one_event(group_df, last_elo_map)
            for _, row in result_df.iterrows():
                p_id = row["p_id"]
                e_value = row["new_elo"]
                e_delta = row["delta"]
                d_id = row["d_id"]
                rows_to_insert.append((p_id, e_value, e_delta))
                # Update last Elo value for this driver for subsequent events
                last_elo_map[d_id] = e_value

        if rows_to_insert:
            insert_stmt = text(
                f"""
                INSERT INTO {write_table} (p_p_id, e_value, e_delta)
                VALUES (:p_id, :val, :delta)
                ON CONFLICT (p_p_id) DO UPDATE 
                SET e_value = EXCLUDED.e_value,
                    e_delta = EXCLUDED.e_delta
            """
            )
            conn.execute(
                insert_stmt,
                [{"p_id": r[0], "val": r[1], "delta": r[2]} for r in rows_to_insert],
            )

        print(
            f"Elo calculation for '{table_name}' completed. Rows inserted/updated: {len(rows_to_insert)}"
        )


if __name__ == "__main__":
    load_dotenv()
    db_url = os.getenv("PG_DATABASE_URL")
    if not db_url:
        print("Error: PG_DATABASE_URL not found in .env")
        sys.exit(1)

    if len(sys.argv) < 2:
        print("Usage: uv run update_elo.py <events/heats>")
        sys.exit(1)

    table_name = sys.argv[1]
    assert table_name in {"events", "heats"}, "Only events and heats are valid options."

    run_elo_calculation(db_url, table_name)
