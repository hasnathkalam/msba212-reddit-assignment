import os
import time
import logging
from typing import List, Optional, Dict, Any

import praw
import pandas as pd
from dotenv import load_dotenv

# ----------------------------- Logging -----------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("reddit_assignment")

# ------------------------- Required Columns ------------------------
REQUIRED_COLUMNS = [
    "title", "score", "upvote_ratio", "num_comments", "author", "subreddit",
    "url", "permalink", "created_utc", "is_self", "selftext", "flair",
    "domain", "search_query"
]

# ------------------------- Reddit Collector ------------------------
class RedditCollector:
    def __init__(self, env_path: str):
        if not os.path.exists(env_path):
            raise FileNotFoundError(f"env file not found at: {env_path}")
        load_dotenv(env_path)

        client_id = os.getenv("REDDIT_CLIENT_ID")
        client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        user_agent = os.getenv("REDDIT_USER_AGENT")

        missing = [k for k, v in {
            "REDDIT_CLIENT_ID": client_id,
            "REDDIT_CLIENT_SECRET": client_secret,
            "REDDIT_USER_AGENT": user_agent
        }.items() if not v]
        if missing:
            raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        self.rows: List[Dict[str, Any]] = []

    @staticmethod
    def _row_from_submission(sub, subreddit_name: str, search_query: Optional[str]) -> Dict[str, Any]:
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
            selftext = selftext[:500]
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
            "search_query": search_query or ""
        }

    def fetch_hot_posts(self, subreddits: List[str], limit_per_sub: int = 50, pause: float = 0.0) -> None:
        total = 0
        for name in subreddits:
            log.info(f"Collecting HOT posts from r/{name} (limit={limit_per_sub})")
            try:
                for sub in self.reddit.subreddit(name).hot(limit=limit_per_sub):
                    self.rows.append(self._row_from_submission(sub, name, None))
                    total += 1
                if pause:
                    time.sleep(pause)
            except Exception as e:
                log.warning(f"Skipping r/{name} due to error: {e}")
        log.info(f"Collected {total} HOT posts.")

    def search_posts(self, query: str, subreddits: List[str], limit_per_sub: int = 50, pause: float = 0.0) -> int:
        total = 0
        for name in subreddits:
            log.info(f'Searching r/{name} for "{query}" (limit={limit_per_sub})')
            try:
                for sub in self.reddit.subreddit(name).search(query, limit=limit_per_sub):
                    self.rows.append(self._row_from_submission(sub, name, query))
                    total += 1
                if pause:
                    time.sleep(pause)
            except Exception as e:
                log.warning(f"Search failed for r/{name}: {e}")
        log.info(f'Search collected {total} posts for query "{query}".')
        return total

    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.rows) if self.rows else pd.DataFrame(columns=REQUIRED_COLUMNS)
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        return df[REQUIRED_COLUMNS]

    def export_posts_csv(self, out_path: str) -> pd.DataFrame:
        df = self.to_dataframe()
        before = len(df)
        if "permalink" in df.columns:
            df = df.drop_duplicates(subset=["permalink"], keep="first")
        after = len(df)
        if before != after:
            log.info(f"Deduplicated {before - after} duplicate posts.")
        df.to_csv(out_path, index=False, encoding="utf-8")
        log.info(f"Saved {len(df)} posts to {out_path}.")
        return df

# ----------------------- Comment Collection ------------------------
def collect_comments_for_posts(reddit: praw.Reddit,
                               post_df: pd.DataFrame,
                               out_csv: str,
                               max_comments_per_post: int = 100) -> pd.DataFrame:
    all_comments = []
    for _, row in post_df.iterrows():
        permalink = row.get("permalink")
        title = row.get("title")
        subreddit_name = row.get("subreddit")
        if not permalink:
            continue
        try:
            submission = reddit.submission(url=permalink)
            submission.comments.replace_more(limit=0)
            flat = submission.comments.list()
            if max_comments_per_post and max_comments_per_post > 0:
                flat = flat[:max_comments_per_post]
            for c in flat:
                all_comments.append({
                    "post_title": title,
                    "subreddit": subreddit_name,
                    "post_permalink": permalink,
                    "comment_id": getattr(c, "id", None),
                    "author": getattr(getattr(c, "author", None), "name", None),
                    "body": getattr(c, "body", "")[:2000] if hasattr(c, "body") else "",
                    "score": getattr(c, "score", None),
                    "created_utc": int(getattr(c, "created_utc", 0)) if getattr(c, "created_utc", None) else None,
                    "depth": getattr(c, "depth", 0)
                })
        except Exception as e:
            log.warning(f"Could not fetch comments for {permalink}: {e}")

    dfc = pd.DataFrame(all_comments)
    dfc.to_csv(out_csv, index=False, encoding="utf-8")
    log.info(f"Saved {len(dfc)} comments to {out_csv}.")
    return dfc

# ------------------------------ Main -------------------------------
if __name__ == "__main__":
    # Paths (adjust if needed)
    ENV_PATH = "/content/drive/MyDrive/Reddit_Assignment/reddit.env"
    POSTS_CSV = "/content/drive/MyDrive/Reddit_Assignment/reddit_data.csv"
    COMMENTS_CSV = "/content/drive/MyDrive/Reddit_Assignment/reddit_comments.csv"

    # Target subreddits and queries
    SUBREDDITS = ["netflix", "movies", "cordcutters", "NetflixBestOf", "television", "Streaming"]
    SEARCH_QUERIES = [
        "Netflix Originals",
        "pricing OR price increase"
    ]

    rc = RedditCollector(ENV_PATH)

    # Task 1: HOT posts
    rc.fetch_hot_posts(SUBREDDITS, limit_per_sub=50, pause=0.0)

    # Task 2: Keyword-based search
    for q in SEARCH_QUERIES:
        rc.search_posts(q, SUBREDDITS, limit_per_sub=30, pause=0.0)

    # Task 3: Export posts
    posts_df = rc.export_posts_csv(POSTS_CSV)

    # Comments: export for sentiment analysis
    comments_df = collect_comments_for_posts(rc.reddit, posts_df, COMMENTS_CSV, max_comments_per_post=100)

    # Summaries
    try:
        log.info(f"Unique subreddits: {sorted(posts_df['subreddit'].dropna().unique().tolist())}")
        log.info(f"Total posts exported: {len(posts_df)}")
        log.info(f"Total comments exported: {len(comments_df)}")
        # Search coverage check
        blank = posts_df["search_query"].fillna("").eq("").sum()
        nonblank = len(posts_df) - blank
        log.info(f'Rows with search_query set: {nonblank}, empty: {blank}')
    except Exception:
        pass
