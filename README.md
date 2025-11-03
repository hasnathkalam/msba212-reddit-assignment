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
