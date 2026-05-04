import pandas as pd
import sqlalchemy as sa
from pathlib import Path

DB_PATH = Path("./f1_dashboard.db")

EXPORT_DIR = Path("./powerbi_exports")

ENGINE = sa.create_engine(f"sqlite:///{DB_PATH}")


def export_driver_scores():
    df = pd.read_sql("SELECT * FROM driver_scores", ENGINE)

    col_order = [
        "consistency_rank", "driver_name", "driver_code", "team_name",
        "consistency_score", "cpi", "points_per_race", "total_points",
        "total_races", "s_lap", "s_position", "s_teammate", "s_reliability"
    ]

    col_order = [c for c in col_order if c in df.columns]
    remaining = [c for c in df.columns if c not in col_order]
    df = df[col_order + remaining]

    path = EXPORT_DIR / "driver_scores.csv"
    df.to_csv(path, index=False)
    return df


def export_race_results():
    df = pd.read_sql("""
        SELECT
            r.race_name,
            r.race_date,
            r.country,
            r.circuit,
            r.round_number,
            rr.driver_code,
            rr.driver_name,
            rr.team_name,
            rr.grid_position,
            rr.finish_position,
            rr.points,
            rr.finished,
            rr.status
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        ORDER BY r.round_number, rr.finish_position
    """, ENGINE)

    df["finished"] = df["finished"].map({1: "Finished", 0: "DNF"})

    path = EXPORT_DIR / "race_results.csv"
    df.to_csv(path, index=False)
    return df


def export_laps():
    df = pd.read_sql("""
        SELECT
            r.race_name,
            r.round_number,
            r.circuit,
            l.Driver        AS driver_code,
            l.LapNumber     AS lap_number,
            l.lap_time_s,
            l.sector1_s,
            l.sector2_s,
            l.sector3_s,
            l.Compound      AS compound,
            l.TyreLife      AS tyre_life,
            l.lap_delta_pct,
            l.driver_median_lap_s,
            l.is_pit_lap
        FROM laps l
        JOIN races r ON l.race_id = r.race_id
        ORDER BY r.round_number, l.Driver, l.LapNumber
    """, ENGINE)

    df["is_pit_lap"] = df["is_pit_lap"].map({1: "Pit Lap", 0: "Clean Lap"})

    path = EXPORT_DIR / "laps.csv"
    df.to_csv(path, index=False)
    return df


def export_races():
    df = pd.read_sql("""
        SELECT
            race_id,
            round_number,
            race_name,
            circuit,
            country,
            race_date
        FROM races
        ORDER BY round_number
    """, ENGINE)

    path = EXPORT_DIR / "races.csv"
    df.to_csv(path, index=False)
    return df


def run_export():

    if not DB_PATH.exists():
        print(f"\nERROR: Database not found at {DB_PATH.resolve()}")
        print("Please run f1_pipeline.py first.")
        return

    EXPORT_DIR.mkdir(exist_ok=True)

    export_driver_scores()
    export_race_results()
    export_laps()
    export_races()


if __name__ == "__main__":
    run_export()
