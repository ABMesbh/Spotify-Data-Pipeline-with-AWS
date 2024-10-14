import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    cilent_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=cilent_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlists = sp.user_playlists('spotify')
    
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    
    spotify_data = sp.playlist_tracks(playlist_URI)   
    
    client = boto3.client('s3')
    
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    
    client.put_object(
        Bucket="spotify-etl-mesbah",
        Key="raw_data/" + filename,
        Body=json.dumps(spotify_data)
        )
    
    glue = boto3.client("glue")
    glue_job_name = "spotify_spark_transformation_job"
    
    try: 
        run_id = glue.start_job_run(JobName = glue_job_name)
        status = glue.get_job_run(JobName=glue_job_name, RunId = run_id['JobRunId'])
        print("Job Status :", status['JobRun']['JobRunState'])
    except Exception as e :
        print(e)