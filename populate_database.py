import psycopg2
import pandas as pd

# connect to an existing database
conn = psycopg2.connect(database="talkingdata", user='postgres', password="*****")
conn.autocommit = True

# open a cursor to perform a database operation
cur = conn.cursor()

# populate all the tables
gender_age = (pd.read_csv('data/raw/gender_age_train.csv')
              .drop_duplicates(subset='device_id')
              .reset_index(drop=True))
phone = (pd.read_csv('data/raw/phone_brand_device_model.csv')
         .merge(gender_age, how='inner', on='device_id')
         .drop_duplicates(subset='device_id')
         .reset_index(drop=True))
events = (pd.read_csv('data/raw/events.csv')
          .merge(gender_age, how='inner', on='device_id'))
app_events = (pd.read_csv('data/raw/app_events.csv')
              .merge(events, how='inner', on='event_id'))
app_labels = (pd.read_csv('data/raw/app_labels.csv')
              .merge(app_events, how='inner', on='app_id'))

for col in gender_age.columns:
    gender_age[col] = gender_age[col].apply(str)

for i in range(gender_age.shape[0]):
    cur.execute("INSERT INTO gender_age VALUES (%s, %s, %s, %s);",
                                                       (gender_age.ix[i][0],
                                                        gender_age.ix[i][1],
                                                        gender_age.ix[i][2],
                                                        gender_age.ix[i][3] ) )

for col in phone.columns:
    phone[col] = phone[col].apply(str)

for i in range(phone.shape[0]):
    cur.execute("INSERT INTO phone_brand_device_model VALUES (%s, %s, %s);",
                                                       (phone.ix[i][0],
                                                        phone.ix[i][1],
                                                        phone.ix[i][2]) )

for col in events.columns:
    events[col] = events[col].apply(str)

for i in range(events.shape[0]):
    cur.execute("INSERT INTO events VALUES (%s, %s, %s, %s, %s);",
                                                        (events.ix[i][0],
                                                        events.ix[i][1],
                                                        events.ix[i][2],
                                                        events.ix[i][3],
                                                        events.ix[i][4]) )

for col in app_events.columns:
    app_events[col] = app_events[col].apply(str)

for i in range(app_events.shape[0]):
    cur.execute("INSERT INTO app_events VALUES (%s, %s, %s, %s);",
                                                       (app_events.ix[i][0],
                                                        app_events.ix[i][1],
                                                        app_events.ix[i][2],
                                                        app_events.ix[i][3] ) )


for col in app_labels.columns:
    app_labels[col] = app_labels[col].apply(str)

for i in range(app_labels.shape[0]):
    cur.execute("INSERT INTO app_labels VALUES (%s, %s);",
                                                       (app_labels.ix[i][0],
                                                        app_labels.ix[i][1]) )
