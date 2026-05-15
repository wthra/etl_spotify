-- dim_album: one row per unique album (keyed by album_name + artist_name)
-- DISTINCT ON ensures exactly 1 row per (album_name, artist_name) even if
-- the same album appears with different label/genre across tracks in source.
{{
    config(
        materialized='table',
        unique_key='album_id'
    )
}}

with source as (
    select
        album_name,
        artist_name,
        label,
        genre,
        release_date
    from {{ ref('stg_tracks_typed') }}
    where album_name is not null
),

-- Pick the most-streamed version of each album when attributes conflict
deduped as (
    select distinct on (album_name, artist_name)
        album_name,
        artist_name,
        label,
        genre,
        release_date
    from source
    order by album_name, artist_name, release_date
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['album_name', 'artist_name']) }} as album_id,
        {{ dbt_utils.generate_surrogate_key(['artist_name']) }}               as artist_id,
        album_name,
        artist_name,
        label,
        genre,
        release_date,
        extract(year from release_date)::int                                   as release_year,
        now()                                                                   as updated_at
    from deduped
)

select * from final
