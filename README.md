# Reddit API Data Collection Pipeline — Netflix Conversations

**Student:** Hasnath Kalam  
**Course:** MSBA 212 – Social Media Analytics  
**Instructor:** Professor Joseph Richards  
**Institution:** California State University, Sacramento  

---

## Assignment Overview
This project builds an end-to-end Python pipeline for analyzing Reddit discussions about **Netflix pricing, content, and audience sentiment**.  
Using the **PRAW (Python Reddit API Wrapper)** library, the program connects securely to Reddit, retrieves both *hot* and keyword-based posts from several Netflix-related subreddits, processes the data with pandas, removes duplicates, handles missing values, and exports a clean CSV dataset for social-media analytics or text-mining tasks.  

**Subreddits covered:**  
`r/netflix`, `r/movies`, `r/cordcutters`, `r/NetflixBestOf`, `r/television`, `r/Streaming`

---

## How to Use
1. **Environment Setup**  
Place a file named `reddit.env` in the same folder as `reddit_code.py` and include:
 
`REDDIT_CLIENT_ID=“your_client_id”

REDDIT_CLIENT_SECRET=“your_client_secret”

REDDIT_USER_AGENT=“your_user_agent”`

** Did not** upload this file to GitHub or not shared it publicly. It is excluded by `.gitignore` for security reasons, as per the assignment gudiance. 
------------------

### 2. Install Dependencies

## System Requirements
- **Python:** 3.9 or later  
- **Libraries:** `praw`, `pandas`, `python-dotenv`  
- **Runtime:** Google Colab or local Python environment

 * Someone can run the project either on Google Colab or in a local Python environment (e.g., VS Code, Anaconda, or terminal) — both will produce the same reddit_data.csv output*
----------------------

## Output Description

The program generates a structured dataset named **`reddit_data.csv`**, which contains Reddit posts related to Netflix pricing, shows, and user sentiment.  
Each record in the CSV file represents a single Reddit post extracted from one of the target subreddits. The dataset is cleaned to remove duplicates, truncated for long text fields, and standardized for analytical use.

| Column | Description |
|:--|:--|
| **title** | The headline or title of the Reddit post. |
| **score** | Net upvotes (total upvotes minus downvotes). |
| **upvote_ratio** | Fraction of upvotes out of total votes (ranges from 0 to 1). |
| **num_comments** | Total number of comments under the post. |
| **author** | Username of the Reddit post creator. |
| **subreddit** | Name of the subreddit from which the post was collected. |
| **url** | External link shared in the post, if available. |
| **permalink** | Direct URL to the Reddit post itself. |
| **created_utc** | UNIX timestamp indicating when the post was created. |
| **is_self** | Boolean flag indicating if the post contains text (`True`) or only a link (`False`). |
| **selftext** | Main body text of the post (trimmed to 500 characters). |
| **flair** | Community-assigned tag or category label (if available). |
| **domain** | Domain of any linked content (e.g., youtube.com, netflix.com). |
| **search_query** | The search term used to collect the post (e.g., “Netflix Originals”). |

All text is encoded in UTF-8 and stored in CSV format, ensuring compatibility witS
## This dataset serves as a foundation for further analytics tasks such as:
- Sentiment and polarity classification using NLP models.  
- Topic modeling using LDA or LSA.  
- Trend tracking of Netflix discussions over time.  
- Cross-subreddit comparison of engagement and sentiment levels.


