#%%
#======================================================================================
#                           FETCH AND INSERT CURRENT DAY FANMATCH DATA
#======================================================================================

#%%
#Libraries under use
import os
import re
import time
import pandas as pd
import kenpompy.FanMatch as kf
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

#%%
# Team Lookup
def build_team_lookup(supabase):
    lookup = {}

    # Base KP teams
    res = supabase.table("teams").select("team_id, team_name").execute()
    for r in res.data:
        lookup[r["team_name"]] = r["team_id"]

    # Aliases -> canonical KP team_id
    res = supabase.table("team_aliases").select("alias_name, canonical_team_id").execute()
    for r in res.data:
        lookup[r["alias_name"]] = r["canonical_team_id"]

    return lookup

#%%
#Arena Lookup
def lookup_arena_id(supabase, arena_name):
    if not arena_name:
        return None
    
    resp = supabase.table("arenas") \
        .select("arena_id, team_id") \
        .eq("arena_name", arena_name) \
        .execute()
    lst = [(row["arena_id"], row["team_id"]) for row in resp.data]
    return lst


#%%
#Text cleaners
def clean_team_name(name: str):
    if name is None:
        return None
    return re.sub(r'\s*\(\d+\)', '', str(name)).strip()   # Remove "(7)" etc.

def clean_rank(rank):
    if rank is None or str(rank).strip() in ["", "nan", "None"]:
        return "NR"
    return str(rank).strip()

def extract_arena_name(location_text):
    """
    'City, ST Arena Name' → 'Arena Name'
    """
    try:
        # arena name starts after the second space
        return location_text.split(" ", 2)[2].strip()
    except:
        return None

def generate_game_id(date_str, index):
    """
    KM + YYYYMMDD + dash + sequence#
    Example: KM20250118-001
    """
    d = date_str.replace("-", "")
    seq = str(index + 1).zfill(3)
    return f"KM{d}-{seq}"

def parse_score(winner_score, loser_score):
    """Convert separate scores into actual_score string."""
    return f"{winner_score}-{loser_score}"

#Parsing Location
def parse_location(location_text):
    try:
        city_state, arena = location_text.split(" ", 1)
        city, state = city_state.split(",")
        return city.strip(), state.strip(), arena.strip()
    except:
        return None, None, None
    
def parse_arena_name(location_text):
    """
    Extract arena_name from FanMatch location:
    'City, ST Arena Name' → 'Arena Name'
    """
    try:
        # Remove 'City, ST ' part
        return location_text.split(" ", 2)[2].strip()
    except:
        return None


#%%
#Main Function
def insert_fanmatch_to_supabase(date_str, browser):
    team_lookup = build_team_lookup(supabase)

    #This commented line is only for leap year date
    #fm = fMatch(browser, date= date_str)

    fm = kf.FanMatch(browser, date=date_str)
    df = fm.fm_df

    if df is None:
        print(f"Skipping {date_str} because no results found")
        return []

    print(f"\nRetrieved {len(df)} FanMatch rows for {date_str}\n")

    rows_to_insert = []
    rows_missed = []

    for _, row in tqdm(df.iterrows(), total=len(df)):
        
        # Clean names
        winner_name = clean_team_name(row["PredictedWinner"])
        loser_name  = clean_team_name(row["PredictedLoser"])

        # Lookup team_id
        team1_id = team_lookup.get(winner_name)
        team2_id  = team_lookup.get(loser_name)

        if not team1_id:
            rows_missed.append(f"{winner_name} vs {loser_name}")
            continue

        if not team2_id:
            rows_missed.append(f"{winner_name} vs {loser_name}")
            continue

        # Possessions (nullable)
        predicted_possessions = None if pd.isna(row["PredictedPossessions"]) else int(row["PredictedPossessions"])
        predicted_score = None if pd.isna(row["PredictedScore"]) else str(row["PredictedScore"])
        # Predicted Score
        if type(row['PredictedScore']) == float:
            row['PredictedScore'] = None

        # Location parsing
        location_text = row["Location"]
        city, state, arena_name = parse_location(location_text)

        # Arena ID lookup
        home_team = ''
        is_neutral_site = False
        arena_name = parse_arena_name(location_text)
        arena_data = lookup_arena_id(supabase, arena_name)
        if arena_data:
            arena_id = arena_data[0][0]
            home_team_id = arena_data[0][1]

            if team1_id == home_team_id:
                home_team = team1_id
            elif team2_id == home_team_id:
                home_team = team2_id
            else:
                home_team = None
                is_neutral_site = True

        else:
            arena_id = None
            home_team = None
            is_neutral_site = True


        # Final dict
        game_row = {
            "game_date": date_str,
            "team1_id": team1_id,
            "team2_id": team2_id,

            "predicted_winner": team1_id,
            "predicted_loser": team2_id,
            "predicted_score": predicted_score,
            "predicted_possessions": predicted_possessions,

            "location": location_text,
            "home_team_id": home_team,

        }

        rows_to_insert.append(game_row)

    # INSERT INTO SUPABASE
    if rows_to_insert:
        try:
            print(f"Inserting {len(rows_to_insert)} rows…")
            supabase.table("day_schedule").upsert(rows_to_insert).execute()
            print("✅ Insert completed")
            if rows_missed:
                print(f"Skipped {len(rows_missed)} rows of NR matches")
        except Exception as e:
            print(f"⚠️Error: {e}")
    else:
        print("⚠️ No rows to insert (all skipped due to missing data).")



#%%
if __name__ == "__main__":
    # Yesterday's data
    target_date = (date.today()).strftime("%Y-%m-%d")
    insert_fanmatch_to_supabase(target_date, browser)
