import os
import time
import logging
from typing import List, Optional

import praw
import pandas as pd
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("reddit_assignment")


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


class RedditCollector:
    """
    A small class that encapsulates:
      - secure credential loading from .env
      - collecting HOT posts from subreddits
      - keyword SEARCH across subreddits
      - cleaning, deduping and CSV export
    """

    def __init__(self, env_path: str):
        # Load credentials
        if not os.path.exists(env_path):
            raise FileNotFoundError(f".env file not found at: {env_path}")
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

        # Initialize PRAW
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )
        self.rows = []

    @staticmethod
    def _row_from_submission(sub, subreddit_name: str, search_query: Optional[str]) -> dict:
        """Extract rubric-required fields from a PRAW submission."""
        # Defensive access (some attributes can be missing for removed/deleted posts)
        title = getattr(sub, "title", None)
        score = getattr(sub, "score", None)
        upvote_ratio = getattr(sub, "upvote_ratio", None)
        num_comments = getattr(sub, "num_comments", None)
        author_obj = getattr(sub, "author", None)
        author = getattr(author_obj, "name", None) if author_obj else None
        url = getattr(sub, "url", None)
        permalink = getattr(sub, "permalink", None)
        created_utc = getattr(sub, "created_utc", None)
        is_self = getattr(sub, "is_self", None)
        selftext = getattr(sub, "selftext", None)
        if isinstance(selftext, str):
            selftext = selftext[:500]  # truncate to 500 chars as required
        flair = getattr(sub, "link_flair_text", None)
        domain = getattr(sub, "domain", None)

        return {
            "title": title,
            "score": score,
            "upvote_ratio": upvote_ratio,
            "num_comments": num_comments,
            "author": author,
            "subreddit": subreddit_name,
            "url": url,
            "permalink": f"https://www.reddit.com{permalink}" if permalink else None,
            "created_utc": int(created_utc) if created_utc is not None else None,
            "is_self": bool(is_self) if is_self is not None else None,
            "selftext": selftext,
            "flair": flair,
            "domain": domain,
            "search_query": search_query if search_query else "",
        }

    def fetch_hot_posts(self, subreddits: List[str], limit_per_sub: int = 50, pause: float = 0.0):
        """Task 1: Collect HOT posts from each subreddit."""
        total = 0
        for name in subreddits:
            log.info(f"Collecting HOT posts from r/{name} (limit={limit_per_sub})")
            try:
                for sub in self.reddit.subreddit(name).hot(limit=limit_per_sub):
                    self.rows.append(self._row_from_submission(sub, name, search_query=None))
                    total += 1
                if pause:
                    time.sleep(pause)
            except Exception as e:
                log.warning(f"Skipping r/{name} due to error: {e}")
        log.info(f"Collected {total} HOT posts across {len(subreddits)} subreddits.")

    def search_posts(self, query: str, subreddits: List[str], limit_per_sub: int = 50, pause: float = 0.0):
        """Task 2: Keyword search across subreddits; persist provenance in `search_query`."""
        total = 0
        for name in subreddits:
            log.info(f'Searching r/{name} for "{query}" (limit={limit_per_sub})')
            try:
                # Use PRAW search; keep schema identical to HOT rows
                for sub in self.reddit.subreddit(name).search(query, limit=limit_per_sub):
                    self.rows.append(self._row_from_submission(sub, name, search_query=query))
                    total += 1
                if pause:
                    time.sleep(pause)
            except Exception as e:
                log.warning(f"Search failed for r/{name}: {e}")
        log.info(f'Search collected {total} posts for query "{query}".')

    def to_dataframe(self) -> pd.DataFrame:
        """Convert to DataFrame and enforce column order."""
        df = pd.DataFrame(self.rows) if self.rows else pd.DataFrame(columns=REQUIRED_COLUMNS)
        # Enforce required columns (add missing if absent)
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        return df[REQUIRED_COLUMNS]

    def export_csv(self, out_path: str):
        """Task 3: Deduplicate and export to CSV (no index)."""
        df = self.to_dataframe()

        # Dedup: prefer id or permalink if available
        keys = []
        if "permalink" in df.columns:
            keys.append("permalink")
        if "url" in df.columns:
            keys.append("url")

        if keys:
            before = len(df)
            df = df.drop_duplicates(subset=keys, keep="first")
            log.info(f"Deduplicated {before - len(df)} duplicates using {keys}.")

        # Save without index
        df.to_csv(out_path, index=False)
        log.info(f"Saved cleaned dataset to {out_path} with {len(df)} rows.")
