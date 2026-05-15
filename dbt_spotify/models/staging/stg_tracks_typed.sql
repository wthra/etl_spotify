-- Typed + enriched view over the raw staging table loaded by Airflow Task 2.
-- Named differently from the source table to avoid circular reference.
select
    track_id,
    track_name,
    artist_name,
    album_name,
    release_date::date                          as release_date,
    genre,
    duration_ms::bigint                         as duration_ms,
    round(duration_ms / 60000.0, 2)            as duration_min,
    popularity::smallint                        as popularity,
    danceability::numeric(5,3)                  as danceability,
    energy::numeric(5,3)                        as energy,
    key::smallint                               as key,
    loudness::numeric(6,2)                      as loudness,
    mode::smallint                              as mode,
    instrumentalness::numeric(7,4)              as instrumentalness,
    tempo::numeric(7,2)                         as tempo,
    stream_count::bigint                        as stream_count,
    country,
    explicit::boolean                           as explicit,
    label,
    date_trunc('month', release_date::date)     as release_month,
    extract(year from release_date::date)::int  as release_year
from {{ source('spotify_raw', 'stg_spotify_tracks') }}
