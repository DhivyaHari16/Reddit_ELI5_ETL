import praw
import os
from dotenv import load_dotenv
from datetime import datetime,timezone,timedelta
import pandas as pd
import logging
import time

def scrape_from_reddit(ds, **kwargs):
    #print(os.environ.get("CLIENT_ID"))
    try:
        reddit = praw.Reddit(
            client_id=os.environ.get("CLIENT_ID"),
            client_secret=os.environ.get("CLIENT_SEC"),
            user_agent="dhivRedditScrapeBot/v1.0(by u/Visible_Ordinary_404)",
            ratelimit_seconds=5000,
            timeout=120
        )

        if reddit is not None:
            logging.info("Connected to reddit")

        # Define subreddit & get yesterday's timestamp
        subreddit_name = "explainlikeimfive"
        subreddit = reddit.subreddit(subreddit_name)

        # Convert ds (YYYY-MM-DD) to timestamp range
        start_date = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        logging.info(f"Connected to Reddit, scraping for date: {start_date}")
        end_date = start_date.replace(hour=23, minute=59, second=59)

        start_timestamp = int(start_date.timestamp())  # Convert to UTC timestamp
        end_timestamp = int(end_date.timestamp())
        logging.info(start_timestamp)
        logging.info(end_timestamp)


        # Fetch posts & their comments
        posts_data = []

        for post in subreddit.new(limit=50):
            try:
                post = reddit.submission(id=post.id)
                post_time = post.created_utc  
            
                if start_timestamp <= post_time <= end_timestamp:
                    post_data = {
                        "id": post.id,
                        "title": post.title,
                        "body": post.selftext,
                        "category":post.link_flair_text,
                        "author": post.author.name if post.author else "Unknown",
                        "author_id":post.author.id if post.author else "Unknown",
                        "author_createdAt":datetime.fromtimestamp(post.author.created_utc).strftime("%Y-%m-%d %H:%M:%S") if post.author.created_utc else "Unknown",
                        "post_created_utc": datetime.fromtimestamp(post.created_utc).strftime("%Y-%m-%d %H:%M:%S")  if post.created_utc else "Unknown",
                        "upvotes": post.score,
                        "num_comments": post.num_comments,
                        "url": post.url,
                        "comments": [],
                    }
                    logging.info(post.id)
                    # Extract comments
                    post.comments.replace_more(limit=1)  # Load top-level comments
                    for comment in post.comments.list():
                        post_data["comments"].append({
                                "comment_id": comment.id,
                                "author": comment.author.name if comment.author else "Unknown",
                                "author_id":comment.author.id if comment.author else "Unknown",
                                "body": comment.body,
                                "created_utc": datetime.fromtimestamp(comment.created_utc).strftime("%Y-%m-%d %H:%M:%S") if comment.created_utc else "Unknown",
                                "upvotes": comment.score
                        })
                    #print(post_data["title"])
                    posts_data.append(post_data)

                    posts_df = pd.DataFrame(
                        posts_data,
                        columns=[
                            "id",
                            "title",
                            "body",
                            "category",
                            "author",
                            "author_id",
                            "author_createdAt",
                            "post_created_utc",
                            "upvotes",
                            "num_comments",
                            "url",
                            "comments"
                        ],
                    )
                    time.sleep(2)
                elif post_time < start_timestamp:
                    print("Post is too old, stopping iteration.")
                    break  # Stop checking older posts    
            #logging.info(posts_df)
            except:
                logging.error(f"postid {post.id} not found!")

         # Get the directory of the current DAG file
        current_dag_directory = os.path.dirname(os.path.abspath(__file__))
        print(current_dag_directory)

        # Specify the directory where you want to save the CSV file
        output_directory = os.path.join(current_dag_directory, 'output')

        # Create the output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)

        # Create a Pandas DataFrame
        posts_df = pd.DataFrame(posts_data)

        # Save to CSV file in the specified output directory
        csv_path = os.path.join(output_directory, 'scrapped_data1.csv')
        posts_df.to_csv(csv_path, index=False)

    except:

        logging.error("Error connecting to Reddit!")   
        raise
