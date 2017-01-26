import psycopg2
import pandas as pd

# connect to an existing database
conn = psycopg2.connect(database='talkingdata', user='postgres', password='******')
conn.autocommit = True

# open a cursor to perform a database operation
cur = conn.cursor()

# TASK 1: How many males are in the database? --------------------------------
cur.execute("SELECT COUNT(*) FROM gender_age WHERE gender='M';")
cur.fetchall()

# TASK 2: How many males belong to the older group? --------------------------
cur.execute("SELECT COUNT(*) FROM gender_age WHERE label LIKE '%+%' AND gender='M';")
cur.fetchall()

# TASK 3: How many of the older males live in Beijing? -----------------------
cur.execute("""CREATE VIEW older_males AS SELECT * FROM gender_age
               WHERE gender='M' AND label LIKE '%+%';""")
cur.execute("""CREATE VIEW distinct_ids AS
               SELECT DISTINCT ON (device_id) device_id, event_id
               FROM events;""")
cur.execute("""SELECT COUNT(*) FROM older_males, distinct_ids
               WHERE older_males.device_id = distinct_ids.device_id; """)
cur.fetchall()

# TASK 4: Find all users that have a number of events > mean + 0.1*variance --
cur.execute("""CREATE VIEW count_events AS SELECT device_id, COUNT(*) event_id
               FROM events GROUP BY device_id;""")
cur.execute(""" SELECT * FROM count_events WHERE event_id >
                ((SELECT AVG(event_id) FROM count_events) +
                0.1 *(SELECT VARIANCE(event_id) FROM count_events));""")
cur.fetchall()
