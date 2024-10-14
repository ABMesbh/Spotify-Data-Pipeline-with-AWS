-- Create a new database named Spotify_db
Create DATABASE Spotify_db;

-- Create or replace a storage integration named s3_init
-- This integration connects to an S3 bucket
create or replace storage integration s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'STORAGE_AWS_ROLE_ARN'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-mesbah')
    COMMENT = 'Creating connection to s3';

-- Describe the storage integration to verify its creation
DESC integration s3_init;

-- Create or replace a file format named csv_fileformat
-- This format specifies how CSV files should be read
CREATE OR REPLACE file format csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1 
    null_if = ('NULL', 'null')
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    empty_field_as_null = TRUE;

-- Create or replace a stage named spotify_stage
-- This stage points to the S3 bucket and uses the storage integration and file format defined earlier
CREATE or REPLACE stage spotify_stage
    URL = 's3://spotify-etl-mesbah/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_fileformat;

-- List the contents of the artists_data directory in the stage
list @spotify_stage/artists_data/;

-- Create or replace a table named album_table
-- This table will store album data
Create or replace table album_table (
    album_id string,
    album_name string,
    release_date DATE,
    total_tracks INT,
    track_id string, 
    track_name string
);

-- Create or replace a table named artists_table
-- This table will store artist data
Create or replace table artists_table (
    artist_id string,
    artist_name string,
    external_urls string
);

-- Create or replace a table named songs_table
-- This table will store song data
create or replace table songs_table (
    track_id string,
    name string,
    duration_ms INT,
    popularity INT,
    spotify_url string,
    album_id string,
    artist_id string,
    artist_name string
);

-- Copy data into album_table from the specified CSV file in the stage
Copy into album_table 
from @spotify_stage/albums_data/albums-transformed2024-10-14.csv/
-- FILE_FORMAT = (FORMAT_NAME = 'csv_fileformat')
-- ON_ERROR = 'Continue';

-- Copy data into artists_table from the specified CSV file in the stage
Copy into artists_table 
from @spotify_stage/artists_data/artists-transformed2024-10-14.csv/;

-- Copy data into songs_table from the specified CSV file in the stage
Copy into songs_table 
from @spotify_stage/songs_data/songs-transformed2024-10-14.csv/;

-- Select all data from songs_table to verify the data load
select * from songs_table;

-- Create or replace a schema named pipe
-- This schema will contain Snowpipe definitions
Create or replace Schema pipe;

-- Create or replace a Snowpipe named songs_table_pipe
-- This pipe will automatically ingest data into songs_table from the specified stage location
create or replace pipe spotify_db.pipe.songs_table_pipe
auto_ingest = true
as 
copy into spotify_db.public.songs_table
from @spotify_db.public.spotify_stage/songs_data/;

-- Create or replace a Snowpipe named artists_table_pipe
-- This pipe will automatically ingest data into artists_table from the specified stage location
create or replace pipe spotify_db.pipe.artists_table_pipe
auto_ingest = true
as 
copy into spotify_db.public.artists_table
from @spotify_db.public.spotify_stage/artists_data/;

-- Create or replace a Snowpipe named albums_table_pipe
-- This pipe will automatically ingest data into album_table from the specified stage location
create or replace pipe spotify_db.pipe.albums_table_pipe
auto_ingest = true
as 
copy into spotify_db.public.album_table
from @spotify_db.public.spotify_stage/albums_data/;

-- Describe the Snowpipe named songs_table_pipe to verify its creation
DESC pipe pipe.songs_table_pipe;

-- Count the number of records in songs_table to verify data ingestion
select count(*) from songs_table;

-- Check the status of the Snowpipe named songs_table_pipe
select system$pipe_status('pipe.songs_table_pipe');