import os
import time
import logging
from typing import List, Optional, Dict, Any

import pandas as pd
import praw
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("reddit_pipeline")

REQUIRED_COLUMNS = [
    "title",
    "score",
    "upvote_ratio",
    "num_comments",
    "author",
    "subreddit",
    "url",
    "permalink",
    "created_utc",
    "is_self",
    "selftext",
    "flair",
    "domain",
    "search_query",
]

def _safe_int(x):
    try:
        return int(x) if x is not None else None
    except Exception:
        return None

def _safe_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

class RedditCollector:
    # Task 1: Secure API initialization from .env (no hard-coded secrets)
    def __init__(self, env_path: str = "reddit.env"):
        if not os.path.exists(env_path):
            raise FileNotFoundError(f"Environment file not found: {env_path}")
        load_dotenv(env_path)

        client_id = os.getenv("REDDIT_CLIENT_ID")
        client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        user_agent = os.getenv("REDDIT_USER_AGENT")
        missing = [k for k, v in {
            "REDDIT_CLIENT_ID": client_id,
            "REDDIT_CLIENT_SECRET": client_secret,
            "REDDIT_USER_AGENT": user_agent,
        }.items() if not v]
        if missing:
            raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )
        self.rows: List[Dict[str, Any]] = []
        self._hot_counts: Dict[str, int] = {}
        self._search_counts: Dict[str, int] = {}

    @staticmethod
    def _row_from_submission(sub, subreddit_name: str, search_query: Optional[str]) -> Dict[str, Any]:
        author_obj = getattr(sub, "author", None)
        selftext = getattr(sub, "selftext", None)
        if isinstance(selftext, str):
            selftext = selftext[:500]

        permalink = getattr(sub, "permalink", None)
        if permalink and not str(permalink).startswith("http"):
            permalink = f"https://www.reddit.com{permalink}"

        return {
            "title": getattr(sub, "title", None),
            "score": _safe_int(getattr(sub, "score", None)),
            "upvote_ratio": _safe_float(getattr(sub, "upvote_ratio", None)),
            "num_comments": _safe_int(getattr(sub, "num_comments", None)),
            "author": getattr(author_obj, "name", None) if author_obj else None,
            "subreddit": subreddit_name,
            "url": getattr(sub, "url", None),
            "permalink": permalink,
            "created_utc": _safe_int(getattr(sub, "created_utc", None)),
            "is_self": bool(getattr(sub, "is_self", False)),
            "selftext": selftext,
            "flair": getattr(sub, "link_flair_text", None),
            "domain": getattr(sub, "domain", None),
            "search_query": search_query or "",
        }

    # Task 2a: Fetch HOT posts for several subreddits
    def fetch_hot_posts(self, subreddits: List[str], limit_per_sub: int = 50, pause_sec: float = 0.0):
        total = 0
        for name in subreddits:
            count = 0
            log.info(f"Collecting HOT posts from r/{name} (limit={limit_per_sub})")
            try:
                for sub in self.reddit.subreddit(name).hot(limit=limit_per_sub):
                    self.rows.append(self._row_from_submission(sub, name, search_query=None))
                    count += 1
                    total += 1
                self._hot_counts[name] = count
                if pause_sec:
                    time.sleep(pause_sec)
            except Exception as e:
                log.warning(f"Skipping r/{name} due to error: {e}")
                self._hot_counts[name] = 0
        log.info(f"Collected {total} HOT posts.")

    # Task 2b: Keyword-based search with provenance in search_query
    def search_posts(self, query: str, subreddits: List[str], limit_per_sub: int = 50, pause_sec: float = 0.0):
        total = 0
        for name in subreddits:
            count = 0
            log.info(f'Searching r/{name} for "{query}" (limit={limit_per_sub})')
            try:
                for sub in self.reddit.subreddit(name).search(query, limit=limit_per_sub):
                    self.rows.append(self._row_from_submission(sub, name, search_query=query))
                    count += 1
                    total += 1
                self._search_counts[name] = count
                if pause_sec:
                    time.sleep(pause_sec)
            except Exception as e:
                log.warning(f'Search failed for r/{name} (query="{query}"): {e}')
                self._search_counts[name] = 0
        log.info(f'Search collected {total} posts.')

    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.rows) if self.rows else pd.DataFrame(columns=REQUIRED_COLUMNS)
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        return df[REQUIRED_COLUMNS]

    # Task 3: Deduplicate and export to CSV without index
    def export_csv(self, out_path: str = "reddit_data.csv") -> pd.DataFrame:
        df = self.to_dataframe()
        before = len(df)
        if "permalink" in df.columns and df["permalink"].notna().any():
            df = df.drop_duplicates(subset=["permalink"], keep="first")
        elif "url" in df.columns and df["url"].notna().any():
            df = df.drop_duplicates(subset=["url"], keep="first")
        removed = before - len(df)
        if removed > 0:
            log.info(f"Deduplicated {removed} row(s).")
        df.to_csv(out_path, index=False)
        log.info(f"Saved {len(df)} rows to {out_path}.")
        return df

    def print_summary(self, df: Optional[pd.DataFrame] = None):
        if df is None:
            df = self.to_dataframe()
        subs = sorted([s for s in df["subreddit"].dropna().unique()])
        log.info(f"Total unique subreddits: {len(subs)}")
        for s in subs:
            log.info(f" - {s}")
        if self._hot_counts:
            log.info("HOT counts by subreddit:")
            for k, v in self._hot_counts.items():
                log.info(f" - r/{k}: {v}")
        if self._search_counts:
            log.info("SEARCH counts by subreddit:")
            for k, v in self._search_counts.items():
                log.info(f" - r/{k}: {v}")
        log.info(f"Rows collected (memory): {len(self.rows)}")
        log.info(f"Rows in DataFrame: {len(df)}")

if __name__ == "__main__":
    SUBS = [
        "netflix",
        "movies",
        "cordcutters",
        "NetflixBestOf",
        "television",
        "Streaming",
    ]
    QUERIES = [
        'Netflix Originals',
        '(pricing OR "price increase" OR plan OR fee) AND (Netflix OR streaming)',
        '(account sharing OR password sharing) AND Netflix',
        '(cancel* OR churn) AND Netflix',
        '(ad tier OR ads OR advertising) AND Netflix',
    ]

    collector = RedditCollector(env_path="reddit.env")
    collector.fetch_hot_posts(SUBS, limit_per_sub=50, pause_sec=0.5)
    for q in QUERIES:
        collector.search_posts(q, SUBS, limit_per_sub=30, pause_sec=0.5)
    df = collector.export_csv(out_path="reddit_data.csv")
    collector.print_summary(df)
