#%%
#======================================================================================
#                               FETCH AND INSERT FANMATCH DATA
#======================================================================================

#%%
#Libraries under use
import os
import re
import time
import pandas as pd
import FanMatch as kf
from datetime import datetime, timedelta, date
from kenpompy.utils import login
from supabase.client import create_client, Client
from tqdm import tqdm

# %%
# --- 1. SETUP & AUTHENTICATION ---
# Using environment variables for GitHub Actions
USERNAME = os.environ.get("KENPOM_USER")
PASSWORD = os.environ.get("KENPOM_PW")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

browser = login(USERNAME, PASSWORD)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 2. HELPER FUNCTIONS ---
def build_team_lookup(supabase):
    lookup = {}
    res = supabase.table("teams").select("team_id, team_name").execute()
    for r in res.data:
        lookup[r["team_name"]] = r["team_id"]
    res = supabase.table("team_aliases").select("alias_name, canonical_team_id").execute()
    for r in res.data:
        lookup[r["alias_name"]] = r["canonical_team_id"]
    return lookup

def lookup_arena_id(supabase, arena_name):
    if not arena_name: return None
    resp = supabase.table("arenas").select("arena_id, team_id").eq("arena_name", arena_name).execute()
    return [(row["arena_id"], row["team_id"]) for row in resp.data]

def clean_team_name(name: str):
    if name is None: return None
    return re.sub(r'\s*\(\d+\)', '', str(name)).strip()

def clean_rank(rank):
    if rank is None or str(rank).strip() in ["", "nan", "None"]: return "NR"
    return str(rank).strip()

def parse_location(location_text):
    try:
        parts = location_text.split(" ", 2)
        arena = parts[2].strip() if len(parts) > 2 else None
        return arena
    except:
        return None

# --- 3. MAIN LOGIC ---
def insert_fanmatch_to_supabase(date_str, browser):
    team_lookup = build_team_lookup(supabase)
    try:
        fm = kf.FanMatch(browser, date=date_str)
        df = fm.fm_df
    except Exception as e:
        print(f"Error fetching FanMatch for {date_str}: {e}")
        return

    if df is None or df.empty:
        print(f"No results for {date_str}")
        return

    rows_to_insert = []
    for _, row in df.iterrows():
        winner_name = clean_team_name(row["Winner"])
        loser_name  = clean_team_name(row["Loser"])
        winner_id = team_lookup.get(winner_name)
        loser_id  = team_lookup.get(loser_name)

        # Clean ranks
        if row['Winner'] == row['Team1']:
            winner_rank = clean_rank(row["Team1Rank"])
            loser_rank  = clean_rank(row["Team2Rank"])
        else:
            winner_rank = clean_rank(row["Team2Rank"])
            loser_rank  = clean_rank(row["Team1Rank"])


        if not winner_id or not loser_id:
            continue

        # Logic for OT and Score
        if row["OT"] == 'nan':
            ot = False 
        else:
            ot = True
            ot_count = 1

        # Location parsing
        arena_name = row["Arena"]
        city = row['City']
        arena_data = lookup_arena_id(supabase, arena_name)
        
        arena_id, home_team, is_neutral = None, None, True
        if arena_data:
            arena_id, home_team_id = arena_data[0]
            if winner_id == home_team_id: home_team = winner_id
            elif loser_id == home_team_id: home_team = loser_id
            is_neutral = False if home_team else True

        game_row = {
            "game_date": date_str,
            "team1_id": winner_id,
            "team1_rank": winner_rank,
            "team2_id": loser_id,
            "team2_rank": loser_rank,
            "winner_id": winner_id,
            "winner_score": int(row["WinnerScore"]),
            "loser_id": loser_id,
            "loser_score": int(row["LoserScore"]),
            "predicted_score": None if pd.isna(row["PredictedScore"]) else row["PredictedScore"],
            "game_total": int(row["WinnerScore"]) + int(row["LoserScore"]),
            "actual_score": f"{row['WinnerScore']}-{row['LoserScore']}",
            "win_probability": str(row["WinProbability"]),
            "predicted_possessions": None if pd.isna(row["PredictedPossessions"]) else int(row["PredictedPossessions"]),
            "actual_possessions": None if pd.isna(row["Possessions"]) else int(row["Possessions"]),
            "ot": ot,
            "OT Count": ot_count,
            "arena_id": arena_id,
            "home_team_id": home_team,
            "is_neutral_site": is_neutral,
            "location_text": city,
        }
        rows_to_insert.append(game_row)

    if rows_to_insert:
        supabase.table("games").upsert(rows_to_insert, on_conflict="game_date, team1_id, team2_id").execute()
        print(f"✅ Successfully processed {len(rows_to_insert)} games for {date_str}")

#%%
if __name__ == "__main__":
    # Yesterday's data
    target_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    insert_fanmatch_to_supabase(target_date, browser)