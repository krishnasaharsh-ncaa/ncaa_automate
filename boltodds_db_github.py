import os
import json
import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from dateutil import parser as dtparser
import websockets
from dotenv import load_dotenv
from trial_to_db_github import upload_from_dfs, get_supabase_client


# ----------------------------
# Config / helpers
# ----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def local_now() -> datetime:
    return datetime.now()

def safe_get(d: Any, key: str, default=None):
    return d.get(key, default) if isinstance(d, dict) else default


def pick_game_date(spreads: pd.DataFrame, totals: pd.DataFrame) -> str:
    if not spreads.empty and "date" in spreads.columns:
        dt = spreads["date"].dropna()
        if not dt.empty:
            return pd.to_datetime(dt).dt.date.mode().iloc[0].strftime("%Y-%m-%d")
    if not totals.empty and "date" in totals.columns:
        dt = totals["date"].dropna()
        if not dt.empty:
            return pd.to_datetime(dt).dt.date.mode().iloc[0].strftime("%Y-%m-%d")
    return date.today().strftime("%Y-%m-%d")


@dataclass
class StoredMessage:
    local_timestamp: str
    server_timestamp: str
    action: str
    data: Dict[str, Any]


# ----------------------------
# Client
# ----------------------------

class BoltOddsWSClient:
    def __init__(
        self,
        api_key_env: str = "BOLT_KEY",
        reconnect_delay_s: int = 5,
        sports: Optional[List[str]] = None,
        sportsbooks: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
    ):
        load_dotenv()
        api_key = os.environ.get('BOLT_KEY')
        if not api_key:
            raise RuntimeError(f"{api_key_env} environment variable not set")

        self.ws_url = f"wss://spro.agency/api?key={api_key}"
        self.reconnect_delay_s = reconnect_delay_s

        self.sports = sports or ["NCAAB"]
        self.sportsbooks = sportsbooks or ["betmgm", "betonline", "draftkings", "fanduel", "pinnacle"]
        self.markets = markets or ["Total", "Spread"]

        self.received: List[StoredMessage] = []
        self._should_reconnect: bool = True
        self._ws = None

        # Main DataFrame
        self.df = pd.DataFrame(columns=[
            "local_timestamp", "server_timestamp", "action", "sport",
            "sportsbook", "game", "extracted_date", "home_team", "away_team",
            "outcome_name", "outcome_target", "outcome_line",
            "outcome_over_under", "odds", "link",
        ])

    def stop(self):
        self._should_reconnect = False

    def _subscription_payload(self) -> Dict[str, Any]:
        return {
            "action": "subscribe",
            "filters": {
                "sports": self.sports,
                "sportsbooks": self.sportsbooks,
                "markets": self.markets
            }
        }

    def _store_message(self, action: str, msg: Dict[str, Any], ts_local: datetime):
        self.received.append(
            StoredMessage(
                local_timestamp=ts_local.isoformat(sep=" ", timespec="seconds"),
                server_timestamp=safe_get(msg, "timestamp", None),
                action=action,
                data=safe_get(msg, "data", {}) or {},
            )
        )

    def _print_summary(self, action: str, data: Dict[str, Any], ts_local: datetime):
        return

    def _append_to_df(self, sm: StoredMessage):
        d = sm.data or {}
        outcomes = safe_get(d, "outcomes", {}) or {}
        
        # --- Extraction Logic ---
        full_game_str = safe_get(d, "game", "")
        extracted_date = None
        if full_game_str and ',' in full_game_str:
            parts = full_game_str.split(',')
            if len(parts) > 1:
                # Strip spaces and convert to date object
                date_str = parts[1].strip()
                try:
                    extracted_date = pd.to_datetime(date_str).date()
                except:
                    extracted_date = None

        base = {
            "local_timestamp": pd.to_datetime(sm.local_timestamp),
            "server_timestamp": pd.to_datetime(sm.server_timestamp, utc=True, errors="coerce"),
            "action": sm.action,
            "sport": safe_get(d, "sport", None),
            "sportsbook": safe_get(d, "sportsbook", None),
            "game": full_game_str,
            "extracted_date": extracted_date, # New Column
            "home_team": safe_get(d, "home_team", None),
            "away_team": safe_get(d, "away_team", None),
        }

        rows = []
        if outcomes:
            for _, o in outcomes.items():
                o = o or {}
                rows.append({
                    **base,
                    "outcome_name": safe_get(o, "outcome_name", None),
                    "outcome_target": safe_get(o, "outcome_target", None),
                    "outcome_line": str(safe_get(o, "outcome_line", None)),
                    "outcome_over_under": safe_get(o, "outcome_over_under", None),
                    "odds": safe_get(o, "odds", None),
                    "link": safe_get(o, "link", None),
                })
        else:
            rows.append(base)

        if rows:
            self.df = pd.concat([self.df, pd.DataFrame(rows)], ignore_index=True)

    async def _handle_message(self, raw: Any):
        ts_local = local_now()
        if raw is None: return
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="replace")
        try:
            msg = json.loads(raw)
            msgs = [msg] if isinstance(msg, dict) else msg
            for data in msgs:
                await self._process_single_message(data, ts_local)
        except Exception: return

    async def _process_single_message(self, msg: Dict[str, Any], ts_local: datetime):
        action = safe_get(msg, "action", "unknown")
        data = safe_get(msg, "data", {}) or {}
        if action in {"initial_state", "game_update", "game_removed", "game_added", "line_update"}:
            self._store_message(action, msg, ts_local)
            self._append_to_df(self.received[-1])
            self._print_summary(action, data, ts_local)

    async def run_snapshot(self, wait_seconds=15):
        async with websockets.connect(self.ws_url, max_size= None) as ws:
            await ws.send(json.dumps(self._subscription_payload()))
            start = asyncio.get_event_loop().time()
            while asyncio.get_event_loop().time() - start < wait_seconds:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    await self._handle_message(msg)
                except asyncio.TimeoutError: continue

# ----------------------------
# Processing: Clubbing Rows Logic
# ----------------------------

def find_current_spreads(df):
    spreads = df[df['outcome_name'] == 'Spread'].copy()
    spreads['odds'] = pd.to_numeric(spreads['odds'], errors='coerce')
    spreads['outcome_line'] = pd.to_numeric(spreads['outcome_line'], errors='coerce')
    spreads = spreads.dropna(subset=['odds', 'outcome_line', 'outcome_target'])
    spreads['abs_spread'] = spreads['outcome_line'].abs()
    
    results = []
    for (game, sportsbook), group in spreads.groupby(['game', 'sportsbook']):
        for abs_val in group['abs_spread'].unique():
            pair = group[group['abs_spread'] == abs_val]
            if len(pair) == 2:
                pair = pair.sort_values('outcome_target')
                p1, p2 = pair.iloc[0], pair.iloc[1]
                results.append({
                    'local_timestamp': p1['local_timestamp'], 'game': game, 'sportsbook': sportsbook,
                    'date': p1['extracted_date'], # Using extracted_date
                    'abs_sum': pair['odds'].abs().sum(),
                    'outcome_target_1': p1['outcome_target'], 'team1_id': p1.get('team_id'), 
                    'spread_1': p1['outcome_line'], 'odds_1': p1['odds'],
                    'outcome_target_2': p2['outcome_target'], 'team2_id': p2.get('team_id'),
                    'spread_2': p2['outcome_line'], 'odds_2': p2['odds']
                })
    res_df = pd.DataFrame(results)
    return res_df.sort_values('abs_sum').groupby(['game', 'sportsbook']).head(1).reset_index(drop=True) if not res_df.empty else res_df

def find_current_totals(df):
    totals = df[df['outcome_name'] == 'Total'].copy()
    totals['odds'] = pd.to_numeric(totals['odds'], errors='coerce')
    totals['outcome_line'] = pd.to_numeric(totals['outcome_line'], errors='coerce')
    totals = totals.dropna(subset=['odds', 'outcome_line', 'outcome_over_under'])
    
    results = []
    for (game, sportsbook), group in totals.groupby(['game', 'sportsbook']):
        for line_val in group['outcome_line'].unique():
            pair = group[group['outcome_line'] == line_val]
            if len(pair) == 2:
                over = pair[pair['outcome_over_under'] == 'O'].iloc[0]
                under = pair[pair['outcome_over_under'] == 'U'].iloc[0]
                results.append({
                    'local_timestamp': over['local_timestamp'], 'game': game, 'sportsbook': sportsbook,
                    'total': line_val, 'date': over['extracted_date'], # Using extracted_date
                    'abs_sum': pair['odds'].abs().sum(),
                    'odds_over': over['odds'], 'odds_under': under['odds'],
                    'home_team_id': over.get('home_team_id'), 'away_team_id': over.get('away_team_id')
                })
    res_df = pd.DataFrame(results)
    return res_df.sort_values('abs_sum').groupby(['game', 'sportsbook']).head(1).reset_index(drop=True) if not res_df.empty else res_df

async def collect_and_process_odds(wait_seconds=20):
    client = BoltOddsWSClient()
    await client.run_snapshot(wait_seconds=wait_seconds)
    client.df['status'] = client.df['odds'].apply(lambda x: 'SUSPENDED' if (pd.isna(x) or x == '') else 'ACTIVE')
    try:
        supabase = get_supabase_client()
        resp = (
            supabase.table("team_aliases")
            .select("canonical_team_id, alias_name, source")
            .eq("source", "BO")
            .execute()
        )
        if resp.data:
            alias_map = {row["alias_name"]: row["canonical_team_id"] for row in resp.data}
        else:
            alias_map = {}
        client.df["team_id"] = client.df["outcome_target"].map(alias_map)
        client.df["home_team_id"] = client.df["home_team"].map(alias_map)
        client.df["away_team_id"] = client.df["away_team"].map(alias_map)
    except Exception:
        alias_map = {}
    client.current_spreads = find_current_spreads(client.df)
    client.current_totals = find_current_totals(client.df)
    return client

async def main():
    wait_seconds = 20
    client = await collect_and_process_odds(wait_seconds=wait_seconds)
    print(f"Retrieved updates for {wait_seconds}s")

    game_date = pick_game_date(client.current_spreads, client.current_totals)
    _, _, found_matches, missed_matches, uploaded_matches = upload_from_dfs(
        client.current_spreads,
        client.current_totals,
        game_date,
        upload=True,
    )

    print(f"Found {found_matches} matches and missed {missed_matches} matches")
    print(f"Uploaded {uploaded_matches} matches to supabase")
    return client

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
