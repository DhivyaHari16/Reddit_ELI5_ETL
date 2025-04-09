import psycopg2
import os
import logging

def insert_to_aggregate(ds, **kwargs):
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
        INSERT INTO kpi_aggregates
        WITH category_data as
        (
            SELECT  DISTINCT p.category, DATE(p.created_at) as post_date,count(post_id) as num_of_posts_in_cat
            FROM posts p
            GROUP BY DATE(p.created_at),p.category
        ),comment_data as 
        (
        SELECT 
                DISTINCT DATE(p.created_at) as post_date,
                c.post_id,
                MIN(c.created_at) AS first_comment_time,
                max(p.num_comments) as comment_count
            FROM comments c
            JOIN posts p 
            ON p.post_id = c.post_id
            GROUP BY c.post_id,post_date
        ), top_users as (
            SELECT DISTINCT DATE(created_at) as post_date,author_id, COUNT(*) AS comment_count,RANK() OVER(PARTITION BY  DATE(created_at) ORDER BY count(*) desc) as rnk
            FROM comments 
            WHERE author_id <> 'Unknown'
            GROUP BY post_date,author_id
        ), top_5_commenters as(
            SELECT *
            FROM top_users
            WHERE rnk <=5
            AND author_id is not null
        ), no_comments as (
            SELECT DISTINCT DATE(created_at) as post_date,sum(num_comments) as total_comments, count(distinct post_id) as num_posts
            , avg(upvotes) as avg_upvotes
            FROM posts
            GROUP BY post_date
        )
        SELECT
            p.created_at::DATE AS kpi_date,
            max(num_cmts.num_posts),
            max(num_cmts.avg_upvotes) AS avg_upvotes_per_post,
            ARRAY_AGG(DISTINCT cat.category) AS most_common_topics,
            CASE 
                WHEN COUNT(DISTINCT p.post_id) = 0 THEN 0 
                ELSE max(num_cmts.total_comments) / max(num_cmts.num_posts) 
            END AS comment_to_post_ratio,
            AVG(cd.first_comment_time - p.created_at) AS avg_time_to_first_comment,
            jsonb_object_agg( cat.category, cat.num_of_posts_in_cat) AS category_counts,
            jsonb_object_agg( t.author_id, t.comment_count) AS top_answerers
            FROM posts p
        INNER JOIN category_data cat ON p.category = cat.category AND date(p.created_at) = cat.post_date
        INNER JOIN no_comments num_cmts ON date(p.created_at) = num_cmts.post_date
        INNER JOIN comment_data cd ON p.post_id = cd.post_id
        INNER JOIN comments c ON p.post_id = c.post_id
        INNER JOIN top_5_commenters t ON c.author_id = t.author_id and DATE(p.created_at) = t.post_date
        WHERE DATE(p.created_at) = '{ds}' 
        GROUP BY DATE(p.created_at)
        ORDER BY DATE(p.created_at)
        """
          
        logging.info("connected to postgres")
        
        cursor.execute(query)
        # Commit changes
        conn.commit()
        logging.info("Data inserted successfully!")
        

    except Exception as e:
        logging.error("Error:", e)
        raise

    finally:
        cursor.close()
        conn.close()

