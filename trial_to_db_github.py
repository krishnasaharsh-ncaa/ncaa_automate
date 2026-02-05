#%%
#Libraries
import os
from supabase.client import create_client, Client
from dotenv import load_dotenv
from datetime import date, datetime, timezone
import pandas as pd

# %%
#Supabase DB
def get_supabase_client() -> Client:
    load_dotenv()
    supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
    return supabase 

#%%
#Match Lookup
def match_lookup(supabase, game_date: str):
    offset = 2
    last_game_date = (
        pd.to_datetime(game_date) + pd.Timedelta(days=offset)
    ).strftime("%Y-%m-%d")
    resp = supabase.table("day_schedule") \
    .select("game_date, team1_id, team2_id") \
    .gte("game_date", game_date) \
    .lte("game_date", last_game_date) \
    .execute()

    if resp.data:
        lookup = { (row["game_date"], row["team1_id"], row["team2_id"]): row for row in resp.data }
    else:
        lookup = {}
    return lookup

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# %%
#Prepare Spreads and Totals for DB
def prepare_spreads(spreads: pd.DataFrame, lookup: dict):
    upsert_payload_spreads = []
    matched = 0
    missed = 0
    for _, row in spreads.iterrows():
        game_date = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")
        if row['team1_id'].startswith('D') or row['team2_id'].startswith('D'):
            continue
        # match to schedule orientation
        key_1 = (game_date, row["team1_id"], row["team2_id"])
        key_2 = (game_date, row["team2_id"], row["team1_id"])

        if key_1 in lookup:
            matched += 1
            t1 = row["team1_id"]
            t2 = row["team2_id"]

            # create TWO side rows
            upsert_payload_spreads.append({
                "game_date": game_date,
                "team1_id": t1,
                "team2_id": t2,
                "sportsbook": row["sportsbook"],
                "market": "spread",
                "side_type": "team_1",   # or "home"/"away" if you can determine
                "team_id": t1,
                "line": float(row["spread_1"]) if pd.notna(row["spread_1"]) else None,
                "odds": int(row["odds_1"]) if pd.notna(row["odds_1"]) else None,
                "line_state": "current",
            })

            upsert_payload_spreads.append({
                "game_date": game_date,
                "team1_id": t1,
                "team2_id": t2,
                "sportsbook": row["sportsbook"],
                "market": "spread",
                "side_type": "team_2",
                "team_id": t2,
                "line": float(row["spread_2"]) if pd.notna(row["spread_2"]) else None,
                "odds": int(row["odds_2"]) if pd.notna(row["odds_2"]) else None,
                "line_state": "current",
            })

        elif key_2 in lookup:
            # swap orientation to match schedule
            matched += 1
            t1 = row["team2_id"]
            t2 = row["team1_id"]

            # when you swap team order, swap spreads/odds too
            upsert_payload_spreads.append({
                "game_date": game_date,
                "team1_id": t1,
                "team2_id": t2,
                "sportsbook": row["sportsbook"],
                "market": "spread",
                "side_type": "team_1",
                "team_id": t1,
                "line": float(row["spread_2"]) if pd.notna(row["spread_2"]) else None,
                "odds": int(row["odds_2"]) if pd.notna(row["odds_2"]) else None,
                "line_state": "current",
            })

            upsert_payload_spreads.append({
                "game_date": game_date,
                "team1_id": t1,
                "team2_id": t2,
                "sportsbook": row["sportsbook"],
                "market": "spread",
                "side_type": "team_2",
                "team_id": t2,
                "line": float(row["spread_1"]) if pd.notna(row["spread_1"]) else None,
                "odds": int(row["odds_1"]) if pd.notna(row["odds_1"]) else None,
                "line_state": "current",
            })

        else:
            missed += 1
    return upsert_payload_spreads, matched, missed


def prepare_totals(totals: pd.DataFrame, lookup: dict):
    upsert_payload_totals = []
    matched = 0
    missed = 0
    for _, row in totals.iterrows():
        game_date = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")
        if row['home_team_id'].startswith('D') or row['away_team_id'].startswith('D'):
            continue
        key_1 = (game_date, row["home_team_id"], row["away_team_id"])
        key_2 = (game_date, row["away_team_id"], row["home_team_id"])

        if key_1 in lookup:
            matched += 1
            t1 = row["home_team_id"]
            t2 = row["away_team_id"]

            total_line = float(row["total"]) if pd.notna(row["total"]) else None

            # create TWO side rows
            upsert_payload_totals.append({
                "game_date": game_date,
                "team1_id": t1,
                "team2_id": t2,
                "sportsbook": row["sportsbook"],
                "market": "total",
                "side_type": "over",   # or "home"/"away" if you can determine
                "team_id": None,
                "line": total_line,
                "odds": int(row["odds_over"]) if pd.notna(row["odds_over"]) else None,
                "line_state": "current",
            })

            upsert_payload_totals.append({
                "game_date": game_date,
                "team1_id": t1,
                "team2_id": t2,
                "sportsbook": row["sportsbook"],
                "market": "total",
                "side_type": "under",
                "team_id": None,
                "line": total_line,
                "odds": int(row["odds_under"]) if pd.notna(row["odds_under"]) else None,
                "line_state": "current",
            })

        elif key_2 in lookup:
            # swap to match schedule orientation
            matched += 1
            t1 = row["away_team_id"]
            t2 = row["home_team_id"]

            total_line = float(row["total"]) if pd.notna(row["total"]) else None

            upsert_payload_totals.append({
                "game_date": game_date,
                "team1_id": t1,
                "team2_id": t2,
                "sportsbook": row["sportsbook"],
                "market": "total",
                "side_type": "over",
                "team_id": None,
                "line": total_line,
                "odds": int(row["odds_over"]) if pd.notna(row["odds_over"]) else None,
                "line_state": "current",
            })

            upsert_payload_totals.append({
                "game_date": game_date,
                "team1_id": t1,
                "team2_id": t2,
                "sportsbook": row["sportsbook"],
                "market": "total",
                "side_type": "under",
                "team_id": None,
                "line": total_line,
                "odds": int(row["odds_under"]) if pd.notna(row["odds_under"]) else None,
                "line_state": "current",
            })

        else:
            missed += 1
    
    return upsert_payload_totals, matched, missed

# %%
#Function to upload to Supabase
def upsert_to_supabase(supabase: Client, payload):
    if not payload:
        return

    try:
        supabase.table("game_market_lines") \
            .upsert(
                payload,
                on_conflict="game_date,team1_id,team2_id,sportsbook,market,side_type,line_state"
            ) \
            .execute()
    except Exception as e:
        raise
    
def build_payloads(spreads: pd.DataFrame, totals: pd.DataFrame, lookup: dict):
    payload_spreads, matched_spreads, missed_spreads = prepare_spreads(spreads, lookup)
    payload_totals, matched_totals, missed_totals = prepare_totals(totals, lookup)
    matched = matched_spreads + matched_totals
    missed = missed_spreads + missed_totals
    return payload_spreads, payload_totals, matched, missed


def upload_payloads(supabase: Client, spreads_payload, totals_payload):
    upsert_to_supabase(supabase, spreads_payload)
    upsert_to_supabase(supabase, totals_payload)


def upload_from_dfs(
    spreads: pd.DataFrame,
    totals: pd.DataFrame,
    game_date: str,
    supabase: Client | None = None,
    upload: bool = True,
):
    
    if supabase is None:
        supabase = get_supabase_client()
    lookup = match_lookup(supabase, game_date)
    spreads_payload, totals_payload, matched, missed = build_payloads(spreads, totals, lookup)

    uploaded = 0
    if upload:
        upload_payloads(supabase, spreads_payload, totals_payload)
        uploaded = matched

    return spreads_payload, totals_payload, matched, missed, uploaded

#%%
if __name__ == "__main__":
    supabase = get_supabase_client()
    spreads = pd.read_csv("current_spreads.csv")
    totals = pd.read_csv("current_totals.csv")
    upload_from_dfs(spreads, totals, date.today().strftime("%Y-%m-%d"), supabase=supabase)
