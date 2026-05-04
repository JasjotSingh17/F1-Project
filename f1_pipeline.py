import fastf1
import pandas as pd
import numpy as np
import sqlalchemy as sa
from pathlib import Path
import warnings
import time

warnings.filterwarnings("ignore")

CACHE_DIR = Path("./f1_cache")
CACHE_DIR.mkdir(exist_ok=True)            
fastf1.Cache.enable_cache(str(CACHE_DIR)) 

DB_PATH = Path("./f1_dashboard.db")

ENGINE = sa.create_engine(f"sqlite:///{DB_PATH}")

SEASON = 2025

MAX_RACES = None   

W_LAP         = 0.35   
W_POSITION    = 0.25   
W_TEAMMATE    = 0.20   
W_RELIABILITY = 0.20   


assert abs((W_LAP + W_POSITION + W_TEAMMATE + W_RELIABILITY) - 1.0) < 1e-9, \
    "ERROR: Weights must sum to 1.0. Check the W_ variables in CONFIGURATION."


def get_race_session(year: int, race_number: int):
    try:
        session = fastf1.get_session(year, race_number, "R")
        session.load(laps=True, telemetry=False, weather=False, messages=False)

        return session

    except Exception as e:
        print(f"  Could not load race {race_number}: {e}")
        return None


def extract_laps(session, race_id: int) -> pd.DataFrame:
    laps = session.laps[[
        "Driver", "LapNumber", "LapTime",
        "Sector1Time", "Sector2Time", "Sector3Time",
        "Compound", "TyreLife", "IsPersonalBest",
        "PitInTime", "PitOutTime", "TrackStatus"
    ]].copy()

    laps["race_id"] = race_id

    return laps


def extract_results(session, race_id: int) -> pd.DataFrame:
    results = session.results[[
        "DriverNumber", "Abbreviation", "FullName",
        "TeamName", "GridPosition", "Position",
        "Status", "Points"
    ]].copy()

    results["race_id"] = race_id

    return results


def timedelta_to_seconds(series: pd.Series) -> pd.Series:
    return pd.to_timedelta(series).dt.total_seconds()


def transform_laps(laps_df: pd.DataFrame) -> pd.DataFrame:
    df = laps_df.copy()   

    df["lap_time_s"] = timedelta_to_seconds(df["LapTime"])
    df["sector1_s"]  = timedelta_to_seconds(df["Sector1Time"])
    df["sector2_s"]  = timedelta_to_seconds(df["Sector2Time"])
    df["sector3_s"]  = timedelta_to_seconds(df["Sector3Time"])
    df["is_pit_lap"] = (
        df["PitInTime"].notna() | df["PitOutTime"].notna()
    )

    df = df[df["lap_time_s"].notna()]


    df = df[~df["TrackStatus"].astype(str).str.contains("[45]", regex=True)]


    df = df[(df["lap_time_s"] > 60) & (df["lap_time_s"] < 300)]

    df["driver_median_lap_s"] = (
        df.groupby("Driver")["lap_time_s"].transform("median")
    )



    df["lap_delta_pct"] = (
        (df["lap_time_s"] - df["driver_median_lap_s"])
        / df["driver_median_lap_s"]
        * 100
    )


    df = df.drop(columns=[
        "LapTime", "Sector1Time", "Sector2Time", "Sector3Time",
        "PitInTime", "PitOutTime"
    ])

    return df


def transform_results(results_df: pd.DataFrame) -> pd.DataFrame:
    df = results_df.copy()
    df = df.rename(columns={
        "DriverNumber": "driver_number",
        "Abbreviation": "driver_code",
        "FullName":     "driver_name",
        "TeamName":     "team_name",
        "GridPosition": "grid_position",
        "Position":     "finish_position",
        "Status":       "status",
        "Points":       "points",
    })


    df["finish_position"] = pd.to_numeric(df["finish_position"], errors="coerce")
    df["grid_position"]   = pd.to_numeric(df["grid_position"],   errors="coerce")
    df["points"]          = pd.to_numeric(df["points"],          errors="coerce")


    df["finish_position"] = df["finish_position"].astype("Int64")
    df["grid_position"]   = df["grid_position"].astype("Int64")

    df["finished"] = df["status"].str.contains(
        r"^\+|^Finished", regex=True, na=False
    )

    return df



def load_to_db(df: pd.DataFrame, table_name: str, if_exists: str = "append"):
    df.to_sql(
        name=table_name,
        con=ENGINE,
        if_exists=if_exists,
        index=False
    )


def run_phase1() -> bool:

    schedule = fastf1.get_event_schedule(SEASON, include_testing=False)
    races = schedule[
        schedule["EventFormat"] == "conventional"
    ].reset_index(drop=True)

    if MAX_RACES:
        races = races.head(MAX_RACES)


    races_dim = pd.DataFrame({
        "race_id":      races["RoundNumber"].tolist(),
        "race_name":    races["EventName"].tolist(),
        "circuit":      races["Location"].tolist(),
        "country":      races["Country"].tolist(),
        "race_date":    races["EventDate"].dt.strftime("%Y-%m-%d").tolist(),
        "round_number": races["RoundNumber"].tolist(),
    })

    load_to_db(races_dim, "races", if_exists="replace")

    all_laps    = []   
    all_results = []   

    for _, row in races.iterrows():

        race_id   = int(row["RoundNumber"])
        race_name = row["EventName"]

        session = get_race_session(SEASON, race_id)
        if session is None:
            continue   

        raw_laps    = extract_laps(session, race_id)
        raw_results = extract_results(session, race_id)

        try:
            clean_laps    = transform_laps(raw_laps)
            clean_results = transform_results(raw_results)

            all_laps.append(clean_laps)
            all_results.append(clean_results)


        except Exception as e:
            print(f"  Transform failed for {race_name}: {e}")

        time.sleep(1)

    if not all_laps:
        print("ERROR: No lap data was collected. Check FastF1 connection.")
        return False

    full_laps = pd.concat(all_laps, ignore_index=True)
    load_to_db(full_laps, "laps", if_exists="replace")

    full_results = pd.concat(all_results, ignore_index=True)
    load_to_db(full_results, "race_results", if_exists="replace")

    return True   

def load_scoring_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    clean_laps = pd.read_sql("""
        SELECT
            race_id,
            Driver               AS driver_code,
            LapNumber            AS lap_number,
            lap_time_s,
            driver_median_lap_s,
            lap_delta_pct,
            Compound             AS compound,
            TyreLife             AS tyre_life
        FROM laps
        WHERE is_pit_lap = 0
          AND lap_time_s IS NOT NULL
    """, ENGINE)

    race_results = pd.read_sql("""
        SELECT
            race_id,
            driver_code,
            driver_name,
            team_name,
            finish_position,
            grid_position,
            points,
            finished,
            status
        FROM race_results
    """, ENGINE)

    return clean_laps, race_results


def compute_s_lap(clean_laps: pd.DataFrame) -> pd.DataFrame:

    per_race = clean_laps.groupby(["driver_code", "race_id"]).agg(
        lap_std    = ("lap_time_s", "std"),
        lap_median = ("lap_time_s", "median"),
        lap_count  = ("lap_time_s", "count"),
    ).reset_index()

    per_race = per_race.dropna(subset=["lap_std"])

    per_driver = per_race.groupby("driver_code").agg(
        mean_lap_std    = ("lap_std",    "mean"),
        mean_lap_median = ("lap_median", "mean"),
        races_counted   = ("race_id",    "count"),
    ).reset_index()

    per_driver["lap_cv"] = (
        per_driver["mean_lap_std"] / per_driver["mean_lap_median"]
    )

    per_driver["s_lap"] = np.clip(1 - per_driver["lap_cv"], 0, 1)

    return per_driver[["driver_code", "s_lap"]]


def compute_s_position(race_results: pd.DataFrame) -> pd.DataFrame:
    df = race_results.copy()

    df["position_for_calc"] = df["finish_position"].where(
        df["finished"] == True, other=20
    )
    df["position_for_calc"] = pd.to_numeric(
        df["position_for_calc"], errors="coerce"
    ).fillna(20)   

    MAX_POSITION = 20   

    per_driver = df.groupby("driver_code").agg(
        position_std  = ("position_for_calc", "std"),
        races_counted = ("race_id",           "count"),
    ).reset_index()

    per_driver["position_std"] = per_driver["position_std"].fillna(0)

    per_driver["s_position"] = np.clip(
        1 - (per_driver["position_std"] / MAX_POSITION), 0, 1
    )

    return per_driver[["driver_code", "s_position"]]


def compute_s_teammate(clean_laps: pd.DataFrame,
                        race_results: pd.DataFrame) -> pd.DataFrame:

    driver_pace = clean_laps.groupby(["driver_code", "race_id"]).agg(
        median_lap = ("lap_time_s", "median")
    ).reset_index()

    driver_teams = race_results[["driver_code", "team_name", "race_id"]].copy()

    teammate_map = driver_teams.merge(
        driver_teams,
        on=["team_name", "race_id"],
        suffixes=("", "_teammate")
    )

    teammate_map = teammate_map[
        teammate_map["driver_code"] != teammate_map["driver_code_teammate"]
    ][["driver_code", "driver_code_teammate", "race_id"]]

    driver_with_pace = teammate_map.merge(
        driver_pace,
        on=["driver_code", "race_id"],
        how="left"
    ).rename(columns={"median_lap": "driver_median"})

    teammate_pace = driver_pace.rename(columns={
        "driver_code": "driver_code_teammate",
        "median_lap":  "teammate_median"
    })

    driver_with_pace = driver_with_pace.merge(
        teammate_pace,
        on=["driver_code_teammate", "race_id"],
        how="left"
    )

    driver_with_pace["gap_to_teammate"] = (
        driver_with_pace["driver_median"] - driver_with_pace["teammate_median"]
    )
    driver_with_pace = driver_with_pace.dropna(subset=["gap_to_teammate"])

    per_driver = driver_with_pace.groupby("driver_code").agg(
        gap_std  = ("gap_to_teammate", "std"),
        gap_mean = ("gap_to_teammate", "mean"),
        n_races  = ("race_id",         "count"),
    ).reset_index()

    per_driver["gap_cv"] = (
        per_driver["gap_std"] / (per_driver["gap_mean"].abs() + 0.001)
    )

    per_driver["s_teammate"] = np.clip(1 - per_driver["gap_cv"], 0, 1)

    return per_driver[["driver_code", "s_teammate"]]


def compute_s_reliability(race_results: pd.DataFrame) -> pd.DataFrame:
    per_driver = race_results.groupby("driver_code").agg(
        races_finished = ("finished", "sum"),    
        total_races    = ("race_id",  "count"),  
    ).reset_index()

    per_driver["s_reliability"] = (
        per_driver["races_finished"] / per_driver["total_races"]
    )

    return per_driver[["driver_code", "s_reliability"]]



def compute_final_scores(clean_laps: pd.DataFrame,
                          race_results: pd.DataFrame) -> pd.DataFrame:
    s_lap         = compute_s_lap(clean_laps)
    s_position    = compute_s_position(race_results)
    s_teammate    = compute_s_teammate(clean_laps, race_results)
    s_reliability = compute_s_reliability(race_results)

    scores = (
        s_lap
        .merge(s_position,    on="driver_code", how="left")
        .merge(s_teammate,    on="driver_code", how="left")
        .merge(s_reliability, on="driver_code", how="left")
    )

    sub_score_cols = ["s_lap", "s_position", "s_teammate", "s_reliability"]
    scores[sub_score_cols] = scores[sub_score_cols].fillna(0)

    scores["consistency_score"] = (
        W_LAP         * scores["s_lap"]         +
        W_POSITION    * scores["s_position"]     +
        W_TEAMMATE    * scores["s_teammate"]     +
        W_RELIABILITY * scores["s_reliability"]
    )

    points_per_race = race_results.groupby("driver_code").agg(
        total_points = ("points",  "sum"),
        total_races  = ("race_id", "count"),
    ).reset_index()
    points_per_race["points_per_race"] = (
        points_per_race["total_points"] / points_per_race["total_races"]
    )

    scores = scores.merge(
        points_per_race[["driver_code", "total_points",
                          "total_races", "points_per_race"]],
        on="driver_code",
        how="left"
    )

    scores["cpi"] = scores["consistency_score"] * scores["points_per_race"]

    driver_info = (
        race_results.sort_values("race_id")
                    .groupby("driver_code").last()
                    .reset_index()
        [["driver_code", "driver_name", "team_name"]]
    )
    scores = scores.merge(driver_info, on="driver_code", how="left")

    scores = scores.sort_values("consistency_score", ascending=False) \
                   .reset_index(drop=True)
    scores["consistency_rank"] = scores.index + 1

    float_cols = ["s_lap", "s_position", "s_teammate", "s_reliability",
                  "consistency_score", "points_per_race", "cpi"]
    scores[float_cols] = scores[float_cols].round(4)

    return scores

def save_driver_scores(scores: pd.DataFrame):
    scores.to_sql(
        name="driver_scores",
        con=ENGINE,
        if_exists="replace",
        index=False
    )


def run_phase2():
    clean_laps, race_results = load_scoring_data()
    scores = compute_final_scores(clean_laps, race_results)
    save_driver_scores(scores)


def run_pipeline():
    phase1_ok = run_phase1()

    if not phase1_ok:
        print("\nPipeline stopped: Phase 1 did not complete successfully.")
        print("Check your internet connection and FastF1 installation.")
        return

    # Run Phase 2
    run_phase2()
    print("\nPipeline complete successfully.")

if __name__ == "__main__":
    run_pipeline()
