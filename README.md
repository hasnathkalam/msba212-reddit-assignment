# Reddit API Data Collection Pipeline – Netflix Conversations
Student: Hasnath Kalam  
Course: MSBA 212 – Social Media Analytics  
Instructor: Professor Joseph Richards  
California State University, Sacramento  

## Assignment Overview
This assignment develops a Python-based Reddit data collection pipeline using the PRAW (Python Reddit API Wrapper) library. The program connects to the Reddit API, retrieves “hot” and keyword-searched posts from Netflix-related subreddits, cleans and deduplicates the data, and exports the results to a structured CSV file for social media analytics.

## How to Run

### 1. Prerequisites
- Python 3.8 or higher  
- Google Colab or Jupyter Notebook  
- Reddit Developer Account (for API credentials)  

### 2. Installation
Install dependencies:
pip install praw pandas python-dotenv  

Or use the provided requirements file:
pip install -r requirements.txt  

### 3. Configuration
Create a text file named reddit.env in the same project folder with the following credentials:
REDDIT_CLIENT_ID     = "YOUR_CLIENT_ID_HERE"  
REDDIT_CLIENT_SECRET = "YOUR_CLIENT_SECRET_HERE"  
REDDIT_USER_AGENT    = "YOUR_USER_AGENT_HERE"  
Save this file locally and do not upload it to GitHub for security reasons.

### 4. Execution
Run the script using either of the following methods:

**Option A – Command Line:**
python reddit_code.py  

**Option B – Google Colab:**
from reddit_code import RedditCollector  
env_path = "/content/drive/MyDrive/Reddit_Assignment/.env"  
out_csv  = "/content/drive/MyDrive/Reddit_Assignment/reddit_data.csv"  
collector = RedditCollector(env_path)  
collector.fetch_hot_posts(["netflix", "movies", "cordcutters"], limit_per_sub=50)  
collector.search_posts("Netflix Originals", ["netflix", "movies"], limit_per_sub=50)  
collector.export_csv(out_csv)  

## Output Description
The program produces a CSV file named reddit_data.csv containing cleaned Reddit post data.  
Columns include:  
- title – Post title  
- score – Net upvotes  
- upvote_ratio – Ratio of upvotes to total votes  
- num_comments – Number of comments  
- author – Username of the post’s author  
- subreddit – Subreddit name  
- url – External link (if applicable)  
- permalink – Direct Reddit post link  
- created_utc – Post creation timestamp  
- is_self – Boolean indicating text-only posts  
- selftext – Post body (truncated to 500 characters)  
- flair – Post flair or tag  
- domain – Linked content domain  
- search_query – Keyword used to find the post  

Duplicate posts are removed using the permalink, and missing fields are represented as NaN.

Author: Hasnath Kalam  
MSBA Candidate | California State University, Sacramento  
GitHub Repository: https://github.com/hasnathkalam/msba212-reddit-assignment
