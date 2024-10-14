import sys
# Import necessary AWS Glue and PySpark libraries
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, col
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime

# Create a Spark context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Define the S3 path for the raw data
s3_path = "s3://spotify-etl-mesbah/raw_data/"

# Create a dynamic frame from the source data in S3
source_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="json",
)

# Convert the dynamic frame to a Spark DataFrame
spotify_df = source_df.toDF()

# Function to process album data
def process_albums(df):
    return df.select(explode("items").alias("item"))\
        .select(col("item.track.album.id").alias("album_id"),
                col("item.track.album.name").alias("album_name"),
                col("item.track.album.release_date").alias("release_date"),
                col("item.track.album.total_tracks").alias("total_tracks"),
                col("item.track.id").alias("track_id"),
                col("item.track.name").alias("track_name"))

# Function to process artist data
def process_artists(df):
    return df.select(explode("items").alias("item")).select(explode("item.track.artists").alias("artist"))\
        .select(col("artist.id").alias("artist_id"),
                col("artist.name").alias("artist_name"),
                col("artist.external_urls.spotify").alias("external_urls")).drop_duplicates()

# Function to process music data
def process_music(df):
    return df.select(explode("items").alias("item"))\
        .withColumn("artist", explode("item.track.artists"))\
        .select(col("item.track.id").alias("track_id"),
                col("item.track.name"),
                col("item.track.duration_ms"),
                col("item.track.popularity"),
                col("item.track.external_urls.spotify").alias("spotify_url"),
                col("item.track.album.id").alias("album_id"),
                col("artist.id").alias("artist_id"),
                col("artist.name").alias("artist_name")
       ).drop_duplicates(["track_id"])

# Process the data
processed_albums = process_albums(spotify_df)
processed_artists = process_artists(spotify_df)
processed_music = process_music(spotify_df)

# Show a sample of the processed albums data
processed_albums.show(5)

# Print the schema of the processed artists and music data
processed_artists.printSchema()
processed_music.printSchema()

# Function to write DataFrame to S3 in the specified format
def write_to_s3(df, path_prefix, format_type="csv"):
    # Convert DataFrame to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    # Write DynamicFrame to S3
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": f"s3://spotify-etl-mesbah/transformed_data/{path_prefix}/"},
        format=format_type
    )

# Write the processed data to S3
write_to_s3(processed_albums, "albums_data/albums{}".format(str(datetime.now().strftime("%Y-%m-%d"))), "csv")
write_to_s3(processed_artists, "artists_data/artists{}".format(str(datetime.now().strftime("%Y-%m-%d"))), "csv")
write_to_s3(processed_music, "songs_data/songs{}".format(str(datetime.now().strftime("%Y-%m-%d"))), "csv")

# Commit the job
job.commit()