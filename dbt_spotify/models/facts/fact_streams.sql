-- fact_streams: grain = one row per track per country
-- Partitioned by release_date, clustered by artist_name & album_id
-- Note: Postgres does not support native partitioning via dbt config;
--       the partition comment documents the intent for BigQuery/Snowflake migration.
{{
    config(
        materialized='table',
        unique_key=['track_id', 'country'],
        indexes=[
            {'columns': ['release_date'],              'type': 'btree'},
            {'columns': ['artist_id'],                 'type': 'btree'},
            {'columns': ['album_id'],                  'type': 'btree'},
            {'columns': ['release_date', 'artist_id'], 'type': 'btree'}
        ]
    )
}}

with tracks as (
    select * from {{ ref('stg_tracks_typed') }}
),

artists as (
    select artist_id, artist_name from {{ ref('dim_artist') }}
),

albums as (
    select album_id, album_name, artist_name from {{ ref('dim_album') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['t.track_id', 't.country']) }} as stream_id,

        -- foreign keys
        t.track_id,
        a.artist_id,
        al.album_id,

        -- descriptors
        t.track_name,
        t.country,
        t.genre,
        t.explicit,

        -- audio features
        t.danceability,
        t.energy,
        t.key,
        t.loudness,
        t.mode,
        t.instrumentalness,
        t.tempo,

        -- measures
        t.duration_ms,
        t.duration_min,
        t.popularity,
        t.stream_count,

        -- date dimensions (for partition-like filtering)
        t.release_date,
        t.release_month,
        t.release_year,

        now() as loaded_at

    from tracks t
    left join artists  a  on a.artist_name = t.artist_name
    left join albums   al on al.album_name = t.album_name
                         and al.artist_name = t.artist_name
)

select * from final
