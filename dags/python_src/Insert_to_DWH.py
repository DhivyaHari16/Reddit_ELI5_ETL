import psycopg2
import pandas as pd
import demjson3
import os
import re 
import logging

def process_and_insert_data(ds, **kwargs):
    try:
        
        # Database connection details
        DB_CONFIG = {
        "database":os.environ.get("DB"), 
        "user":os.environ.get("DB_USER"), 
        "password":os.environ.get("DB_PWD"), 
        "host":os.environ.get("HOST"), 
        "port":os.environ.get("PGDB_PORT")
        }

        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor() 
        # Read data from reddit_posts into a Pandas DataFrame
        query = f"""
            SELECT *
            FROM raw_reddit_posts
            WHERE date(post_created_utc) = '{ds}'
        """
          
        df = pd.read_sql(query, conn)
        logging.info("connected to postgres")
        # Prepare lists for bulk insert
        authors_data = set()
        posts_data = []
        comments_data = []
        comment_authors = set()

        # Process each row
        for _, row in df.iterrows():
            post_id, title ,body,category, author, author_id,author_createdat,post_created_utc, upvotes,num_comments,url, comments = row

            # Insert into authors (avoid duplicates using a set)
            authors_data.add((author_id, author, author_createdat))

            # Insert into posts
            posts_data.append(( post_id, title ,category,body, post_created_utc, author_id,num_comments,upvotes,url))

            # Parse the JSON comments field 
            try:
                dict_matches = re.findall(r'{.*?}', comments)
              
                if dict_matches:
                    for json_string in dict_matches:
                        data = demjson3.decode(json_string)
                        
                        if isinstance(data, dict):
                            comment_id = data.get('comment_id')                            
                            author_id = data.get('author_id')
                            body=data.get('body')
                            created_at=data.get('created_utc')
                            comment_author = data.get('author')
                            if comment_id is not None and post_id is not None and author_id is not None and body is not None and created_at is not None:
                                comments_data.append((comment_id, post_id, author_id, body, created_at))
                                comment_authors.add((author_id, comment_author, None))
                               
                            else:
                                logging.info("One or more values were none.")
                        else:
                            logging.warning("Invalid JSON structure.")
                else:
                    logging.warning(f"Could not find JSON dictionaries in the string. - post_id {post_id}")
            except demjson3.JSONDecodeError as e:
                logging.warning(f"Error decoding JSON For post_id {post_id} {e}")
        
        # Insert authors
        cursor.executemany("""
            INSERT INTO authors (author_id, username, created_at) 
            VALUES (%s, %s,%s) 
            ON CONFLICT (author_id) DO NOTHING;
        """, list(authors_data))

        # Insert comment_authors
        cursor.executemany("""
            INSERT INTO authors (author_id, username, created_at) 
            VALUES (%s, %s,%s) 
            ON CONFLICT (author_id) DO NOTHING;
        """, list(comment_authors))

        # Insert posts
        cursor.executemany("""
            INSERT INTO posts (post_id, title ,category,body, created_at, author_id,num_comments,upvotes,url) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (post_id) DO NOTHING;
        """, posts_data)

        # Insert comments
        if comments_data:
            cursor.executemany("""
                INSERT INTO comments (comment_id, post_id, author_id, body, created_at) 
                VALUES (%s, %s, %s, %s, %s) 
                ON CONFLICT (comment_id) DO NOTHING;
            """, comments_data)
        else:
            logging.info("comments_data is empty")

        # Commit changes
        conn.commit()
        logging.info("Data inserted successfully!")
        

    except Exception as e:
        logging.error("Error:", e)
        raise

    finally:
        cursor.close()
        conn.close()

