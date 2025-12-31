#%%
#Import Libraries
import pandas as pd
import os
from bs4 import BeautifulSoup
from typing import Optional
from kenpompy.utils import get_html
import time
from io import StringIO
from supabase.client import create_client, Client
from datetime import timedelta, datetime, date
import random

#%%
#Authenticate Kenpom
from kenpompy.utils import login


# --- 1. SETUP & AUTHENTICATION ---
# Using environment variables for GitHub Actions
USERNAME = os.environ.get("KENPOM_USER")
PASSWORD = os.environ.get("KENPOM_PW")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

browser = login(USERNAME, PASSWORD)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# %%
#=====================================================================================================
#                                       BOX SCORE CONSTRUCTOR AND METHODS
#=====================================================================================================

#%%
#Box Score Class
class BoxScore:

    def __init__(
        self,
        browser,
        supabase_client,
        start_date: str,
        end_date: Optional[str] = None
    ):
        self.browser = browser
        self.supabase = supabase_client

        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = (
            datetime.strptime(end_date, "%Y-%m-%d")
            if end_date else self.start_date
        )

        self.boxscore_rows = []      # READY for DB upload

    def date_range(self):
        cur = self.start_date
        while cur <= self.end_date:
            yield cur.strftime("%Y-%m-%d")
            cur += timedelta(days=1)
    

    def build_team_lookup(self):
        lookup = {}

        res = self.supabase.table("teams").select("team_id, team_name").execute()
        for r in res.data:
            lookup[r["team_name"]] = r["team_id"]

        res = self.supabase.table("team_aliases") \
            .select("alias_name, canonical_team_id").execute()

        for r in res.data:
            lookup[r["alias_name"]] = r["canonical_team_id"]

        return lookup
    
    def build_game_lookup(self, game_date: str):
        """
        Build a lookup for ONE date only:
        (team1_id, team2_id) -> game_id
        Allows swapped team order.
        """
        res = (
            self.supabase
            .table("games")
            .select("game_id, team1_id, team2_id")
            .eq("game_date", game_date)
            .execute()
        )

        lookup = {}

        for r in res.data:
            lookup[(r["team1_id"], r["team2_id"])] = r["game_id"]
            lookup[(r["team2_id"], r["team1_id"])] = r["game_id"]

        print(f"Loaded {len(lookup)//2} games for {game_date}")
        return lookup

    
    def get_links(self, date_str):
        url = f"https://kenpom.com/fanmatch.php?d={date_str}"
        soup = BeautifulSoup(get_html(self.browser, url), "html.parser")

        table = soup.select_one("#fanmatch-table")
        if not table:
            return {}

        match_links = {}

        for row in table.find_all("tr"):
            links = row.find_all("a", href=True)

            teams = [a.text.strip() for a in links if "team.php?" in a["href"]]
            box = next((a["href"] for a in links if "box.php?" in a["href"]), None)
            if len(teams) == 2 and box:
                match_links[(teams[0], teams[1])] = f"https://kenpom.com/{box}"

        print(f"Found {len(match_links)} match links for {date_str}")
        return match_links


    
    def parse_box_score(self, box_url):
        soup = BeautifulSoup(get_html(browser, box_url), "html.parser")
        table = soup.select_one("#linescore-table2")

        if table is None:
            return None, None

        df = pd.read_html(StringIO(str(table)))[0]
        df.rename(columns={"Unnamed: 0": "Team"}, inplace=True)

        ot_count = max(0, df.shape[1] - 6)

        df["H1"] = df["Q1"] + df["Q2"]
        df["H2"] = df["Q3"] + df["Q4"]
        df["OT"] = df["T"] - df["H1"] - df["H2"] if ot_count > 0 else 0

        rows = []
        for _, r in df.iterrows():
            rows.append({
                "team_name": r["Team"],
                "H1": int(r["H1"]),
                "H2": int(r["H2"]),
                "OT": int(r["OT"])
            })

        return rows, ot_count



    def collect(self):
        self.boxscore_rows = []
        team_lookup = self.build_team_lookup()

        for game_date in self.date_range():
            print(f"\n--- Collecting box scores for {game_date} ---")

            daily_links = self.get_links(game_date)
            if not daily_links:
                print(f"No games found for {game_date}")
                continue

            for (team1, team2), box_url in daily_links.items():
                team1_id = team_lookup.get(team1)
                team2_id = team_lookup.get(team2)

                if not team1_id or not team2_id:
                    continue

                jitter = random.uniform(6, 15)
                time.sleep(jitter)
                try:
                    parsed_rows, ot_count = self.parse_box_score(box_url)
                    if not parsed_rows:
                        continue

                    game_row = {
                        "game_date": game_date,
                        "team1_id": team1_id,
                        "team2_id": team2_id,
                        "H1_T1 Score": None,
                        "H2_T1 Score": None,
                        "OT_T1 Score": None,
                        "H1_T2 Score": None,
                        "H2_T2 Score": None,
                        "OT_T2 Score": None,
                        "OT Count": ot_count,
                    }

                    for r in parsed_rows:
                        tid = team_lookup.get(r["team_name"])
                        if tid == team1_id:
                            game_row["H1_T1 Score"] = r["H1"]
                            game_row["H2_T1 Score"] = r["H2"]
                            game_row["OT_T1 Score"] = r["OT"]
                        elif tid == team2_id:
                            game_row["H1_T2 Score"] = r["H1"]
                            game_row["H2_T2 Score"] = r["H2"]
                            game_row["OT_T2 Score"] = r["OT"]

                    self.boxscore_rows.append(game_row)
                
                except Exception as e:
                    print(f"⚠️ Failed to parse {box_url}: {e}")
                    # Wait a bit longer if we hit an error to "cool down"
                    time.sleep(20)
                    continue
            print(f"Collected {len(self.boxscore_rows)} games so far")

        print(f"\n✅ Total collected box score rows: {len(self.boxscore_rows)}")
        return self.boxscore_rows



    def upload(self, batch_size=500):
        print(f"Uploading {len(self.boxscore_rows)} box scores")

        rows_by_date = {}

        # Group updates by date
        for row in self.boxscore_rows:
            rows_by_date.setdefault(row["game_date"], []).append(row)

        for game_date, rows in rows_by_date.items():
            print(f"Resolving games for {game_date}")
            game_lookup = self.build_game_lookup(game_date)

            updates = []
            skipped = 0

            for row in rows:
                key = (row["team1_id"], row["team2_id"])
                game_id = game_lookup.get(key)

                if not game_id:
                    skipped += 1
                    continue

                update_row = {
                    "game_id": game_id,
                    "H1_T1 Score": row["H1_T1 Score"],
                    "H2_T1 Score": row["H2_T1 Score"],
                    "OT_T1 Score": row["OT_T1 Score"],
                    "H1_T2 Score": row["H1_T2 Score"],
                    "H2_T2 Score": row["H2_T2 Score"],
                    "OT_T2 Score": row["OT_T2 Score"],
                    "OT Count": row["OT Count"],
                }

                updates.append(update_row)

            print(f"{len(updates)} matched, {skipped} skipped for {game_date}")

            # Batch update
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i+batch_size]

                for r in batch:
                    gid = r.pop("game_id")
                    (
                        self.supabase
                        .table("games")
                        .update(r)
                        .eq("game_id", gid)
                        .execute()
                    )



#%%

if __name__ == "__main__":
    target_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    bs = BoxScore(
        browser=browser,
        supabase_client=supabase,
        start_date= target_date,
        end_date= target_date
    )

    checker = bs.collect()
    bs.upload()
    print("All box scores successfully uploaded!")