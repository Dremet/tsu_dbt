#!/usr/bin/env python3
"""
Lädt CSV-Dateien aus einem Ordner in die passenden Tabellen des Schemas
`source`.  Mit `--truncate` können die Tabellen vor dem ersten Import
geleert werden.

Beispiel:
    # erster Import eines Typs  ➜ Tabellen leeren + laden
    uv run python copy_csv_files_into_db.py \
        --truncate /home/data/events/archive/20250313_214747/csv events

    # Folge-Imports desselben Typs ➜ nur laden
    uv run python copy_csv_files_into_db.py \
        /home/data/events/archive/20250314_091337/csv events
"""
import argparse
import os
import sys
import time
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

###############################################################################
# 1) Mapping: Dateinamens-Suffix → Tabellenname
###############################################################################
CSV_SUFFIX_TO_TABLE: Dict[str, str] = {
    "event.checkpoint-results.csv": "json_checkpoint_results",
    "event_details.compounds.csv": "log_compounds",
    "event_details.drivers.csv": "log_drivers",
    "event_details.main.csv": "log_main",
    "event.drivers.csv": "json_drivers",
    "event.event.csv": "json_event",
    "event.fastest-lap-results.csv": "json_fastest_lap_results",
    "event.lap-results.csv": "json_lap_results",
    "event.race-results.csv": "json_race_results",
    # ggf. ergänzen …
}

###############################################################################
# 2) Lock-Datei (verhindert parallele Lauf­zeit­kollisionen)
###############################################################################
LOCKFILE = os.path.join(os.path.dirname(__file__), "RUNNING")


def acquire_lock(skip_lock: bool = False) -> None:
    """Erzwingt Exklusivität per RUNNING-Datei."""
    if skip_lock:
        return
    tries = 0
    while os.path.exists(LOCKFILE):
        if tries >= 100:
            raise TimeoutError("Lockfile bleibt bestehen (100× gewartet) – Abbruch.")
        print("Anderer Import aktiv – warte …")
        time.sleep(3)
        tries += 1
    print("Lock erhalten (RUNNING-Datei angelegt).")
    open(LOCKFILE, "w").close()


def release_lock(skip_lock: bool = False) -> None:
    """Entfernt RUNNING-Datei am Ende."""
    if skip_lock:
        return
    if os.path.exists(LOCKFILE):
        os.remove(LOCKFILE)
        print("Lock freigegeben.")
    else:
        print("Warnung: Lockfile fehlte beim Freigeben.")


###############################################################################
# 3) Import-Routine
###############################################################################
def import_csv_files(
    folder_path: str,
    db_url: str,
    server: str,
    truncate: bool = False,
    skip_lock: bool = False,
) -> None:
    acquire_lock(skip_lock)

    engine = create_engine(db_url)

    # A) Tabellen leeren (nur beim allerersten Import eines Typs)
    if truncate:
        print("TRUNCATE aller bekannten source-Tabellen …")
        with engine.begin() as conn:
            for tbl in CSV_SUFFIX_TO_TABLE.values():
                try:
                    conn.execute(text(f"TRUNCATE source.{tbl};"))
                    print(f"  → source.{tbl} geleert")
                except Exception as exc:  # Tabelle existiert evtl. noch nicht
                    print(f"  → konnte source.{tbl} nicht truncaten ({exc})")

    # B) Alle CSV-Dateien lesen & anhängen
    csv_files: List[str] = sorted(
        f for f in os.listdir(folder_path) if f.endswith(".csv")
    )

    for csv_file in csv_files:
        file_path = os.path.join(folder_path, csv_file)

        # passendes Ziel ermitteln
        target_table = next(
            (tbl for suf, tbl in CSV_SUFFIX_TO_TABLE.items() if csv_file.endswith(suf)),
            None,
        )
        if target_table is None:
            print(f"Überspringe unbekannte Datei: {csv_file}")
            continue

        print(f"→ lade {csv_file}  ➜  source.{target_table}")
        try:
            df = pd.read_csv(file_path)

            # Zusatz-Spalte für json_event
            if target_table == "json_event":
                df["server"] = server

            df.to_sql(
                target_table,
                engine,
                schema="source",
                if_exists="append",
                index=False,
                method="multi",
            )
        except Exception as exc:
            print(f"Fehler bei {csv_file}: {exc}")

    engine.dispose()
    release_lock(skip_lock)
    print("Import abgeschlossen.")


###############################################################################
# 4) CLI-Entry-Point
###############################################################################
def main() -> None:
    load_dotenv()
    db_url = os.getenv("PG_DATABASE_URL")
    if db_url is None:
        print("PG_DATABASE_URL fehlt in .env")
        sys.exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="leert die source-Tabellen vor dem Import",
    )
    parser.add_argument(
        "--skip-lock",
        action="store_true",
        help="überspringt Dateilock (nur für kontrollierte Batch-Runs)",
    )
    parser.add_argument("folder", help="Ordner mit CSV-Dateien")
    parser.add_argument(
        "server",
        choices=["events", "heats", "hotlapping"],
        help="Typ (wird in json_event.server geschrieben)",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.folder):
        print(f"'{args.folder}' ist kein gültiger Ordner.")
        sys.exit(1)

    import_csv_files(
        folder_path=args.folder,
        db_url=db_url,
        server=args.server,
        truncate=args.truncate,
        skip_lock=args.skip_lock,
    )


if __name__ == "__main__":
    main()
