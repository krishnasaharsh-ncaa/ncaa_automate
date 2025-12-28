#%%
#Import Libraries
import pandas as pd
import time
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from supabase.client import create_client, Client
import os
from tqdm import tqdm

# Use os.environ.get directly; GitHub Actions will provide these
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials not found in environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
#%%
# Scrape class
class TRScraper:
    def __init__(self, start_date, end_date = None):
        self.start = datetime.strptime(start_date, "%Y-%m-%d")

        if end_date:
            self.end   = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            self.end = self.start

    def date_range(self):
        """Generator that yields dates from start to end."""
        cur = self.start
        while cur <= self.end:
            yield cur.strftime("%Y-%m-%d")
            cur += timedelta(days=1)


    def scrape_by_date(self, stat, date):
        print("Scraping data for the date:", start_date)
        url = f"https://www.teamrankings.com/ncaa-basketball/stat/{stat}?date={date}"

        try:
            df = pd.read_html(url)[0]
            stat_col = df.columns[2] 

            ret_df = df[['Team', stat_col]].copy()

            ret_df.rename(columns={stat_col: 'value'}, inplace=True)
            ret_df['date'] = date
            ret_df['stat'] = stat
            return ret_df
        except Exception as e:
            print("Failed:", url, e)
            return None

    def scrape_stat(self, stat):
        """Scrape one stat for all dates in range."""
        all_frames = []
        for i, date in enumerate(self.date_range(), start=1):

            # Print every 10 days
            if i % 10 == 0:
                print(f"Progress: Day {i} — currently scraping {date}")

            df = self.scrape_by_date(stat, date)
            if df is not None:
                all_frames.append(df)

            time.sleep(3)

        if all_frames:
            return pd.concat(all_frames, ignore_index=True)
        return None



#%%
#Helper Functions
def alias_info_lookup():
    alias_lookup = {}

    res = supabase.table("team_aliases") \
                .select("alias_name, canonical_team_id") \
                .execute()

    res2 = supabase.table('teams') \
                .select('team_name, team_id') \
                .execute()
    for row in res.data:
        alias_lookup[row["alias_name"]] = row["canonical_team_id"]

    for rows in res2.data:
        alias_lookup[rows["team_name"]] = rows["team_id"]

    print("Loaded alias lookup:", len(alias_lookup))

    return alias_lookup


def clean_value(val):
    """Convert '38.2%' to float 38.2 and handle NaN."""
    if pd.isna(val):
        return None
    s = str(val).strip().replace('%', '')
    try:
        return float(s)
    except:
        return None


def get_season_year(date_str):
    """TeamRankings convention: November 2022 belongs to season 2022."""
    year = int(date_str[:4])
    month = int(date_str[5:7])
    return year if month >= 7 else year - 1
# %%
#Start dates, end dates and stats for automated script
today = date.today()
start_date = today - timedelta(days=1)
start_date = str(start_date)
end_date = start_date

stats = ['three-point-pct', 'two-point-pct', 'free-throw-pct', 'free-throws-made-per-game', 'three-point-rate', 
         'opponent-three-point-pct', 'opponent-two-point-pct', 'opponent-free-throw-pct', 
         'opponent-free-throws-made-per-game', 'opponent-three-point-rate']

stats_d = ['opponent-three-point-pct', 'opponent-two-point-pct', 'opponent-free-throw-pct', 
         'opponent-free-throws-made-per-game', 'opponent-three-point-rate']

#%%
#Start dates, end dates and stats for maunal run
#=============== COMMENTED FOR SCRIPT =======================
# start_date_list = ['2022-11-07', '2023-11-06', '2024-11-04']
# end_date_list = ['2023-04-08', '2024-04-08', '2025-04-15']

# %%
#Main Function for automated script
def scrape_data(stat, start_date, end_date):
    time.sleep(3)
    rows = []
    scrape = TRScraper(start_date=start_date, end_date=end_date)
    df_check = scrape.scrape_stat(stat)

    if df_check is None:
        print(f"Nothing found for the date: {start_date}")
        return

    alias_lookup = alias_info_lookup()
    #print(df_check)

    for _, row in tqdm(df_check.iterrows(), total=len(df_check)):
        team_name = row['Team']

        if team_name not in alias_lookup:
            print("Missing alias:", team_name)
            continue

        team_id = alias_lookup[team_name]
        stat_date = row['date']
        stat_value = clean_value(row['value'])
        season_year = get_season_year(stat_date)
        rows.append({
            "team_id": team_id,
            "stat_name": stat,
            "stat_value": stat_value,
            "stat_date": stat_date,
            "season_year": season_year,
            "source": "TR"
        })
    return rows


# %%
#Uploading in batches for automated script
for stat in stats:
    time.sleep(2)
    print(f"Uploading {stat}")
    rows = scrape_data(stat, start_date , end_date)

    batch_size = 500

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]

        resp = supabase.table("tr_team_daily_stats").upsert(
            batch,
            on_conflict="team_id,stat_name,stat_date"
        ).execute()

        print(f"Inserted batch {i//batch_size+1}")
    print(f"{stat} update successful. Moving to next! \n")

print("All data successfully uploaded!")
