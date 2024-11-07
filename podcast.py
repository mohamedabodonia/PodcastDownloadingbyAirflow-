from airflow.decorators import dag,task 
import pendulum
import requests
import xmltodict
import os
from airflow.provider.sqlite.operators.sqlite import sqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

@dag(
    dag_id='podcast-summery',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022,11,7),
    catchup=False

)
def podcast_summary():
    create_database=sqliteOperator(
        task_id='create_table_sqlite',
        sql=r"""
        CREATE TABLE IF NOT EXISTS epesides (
            link TEXT PRIMARY KEY,
            title TEXT,
            firstname TEXT,
            published TEXT,
            descreption TEXT
        )
        """,
        sqlite_conn_id="podcasts"


    )

    @task
    def getepisode():
        data=requests.get('https://marketplace.org/feed/podcast/marketplace')
        feed=xmltodict.parse(data.text)
        epesides=feed["rss"]["channel"]["item"]
        print(f'found{len(epesides)}epesides')
        return epesides

    podcast_episdos=getepisode()
    create_database.set_downstream(podcast_episdos)
    
    @task
    def load_episodes(episodes):
        hook=SqliteHook(sqlite_conn_id="podcast")
        stored=hook.get_pandas_df("select * from episodes;")
        new_episodes=[]
        for episode in episodes:
            if episodes["link"] not in stored["link"].values:
                filename=f"{episode["link"].spilt('/')[-1]}.mp3"
                new_episodes.append(episode["link"],episode["title"],episode[pubDate],episode["description"],filename)
        hook.insert_row(table=episodes,rows=new_episodes,target_fields=["link","published","description","filename"])
    load_episodes(podcast_episdos)

    @task
    def download_episodes():
        for episode in episode:
            filename=f"{episode["link"].spilt('/')[-1]}.mp3"
            audio_path=os.path.join("episodes",filename)
            if not os.path.exists(audio_path):
                print(f"downloading {filename}")
                audio=requests.get(episode["enclosure"]["@url"])
                with open (audio_path,"wb+") as f :

                    f.write(audio.content)

    download_episodes(podcast_episdos)

summary = podcast_summary()

