-- mart: genre performance rolled up to month
-- Used by DA for trend analysis and playlist strategy
{{
    config(
        materialized='table',
        indexes=[{'columns': ['release_month', 'genre'], 'type': 'btree'}]
    )
}}

with monthly as (
    select
        f.release_month,
        f.release_year,
        f.genre,
        sum(f.stream_count)                     as total_streams,
        avg(f.popularity)::numeric(5,2)         as avg_popularity,
        avg(f.danceability)::numeric(5,3)       as avg_danceability,
        avg(f.energy)::numeric(5,3)             as avg_energy,
        avg(f.tempo)::numeric(7,2)              as avg_tempo,
        count(distinct f.track_id)              as unique_tracks,
        count(distinct f.artist_id)             as unique_artists,
        count(distinct f.country)               as market_reach,
        sum(f.stream_count) * 1.0 /
            sum(sum(f.stream_count)) over (
                partition by f.release_month
            )                                   as genre_share_pct
    from {{ ref('fact_streams') }} f
    where f.genre is not null
    group by 1, 2, 3
),

with_mom_growth as (
    select
        *,
        lag(total_streams) over (
            partition by genre
            order by release_month
        )                                       as prev_month_streams,
        round(
            (total_streams - lag(total_streams) over (
                partition by genre order by release_month
            )) * 100.0 /
            nullif(lag(total_streams) over (
                partition by genre order by release_month
            ), 0),
        2)                                      as mom_growth_pct
    from monthly
)

select * from with_mom_growth
order by release_month desc, total_streams desc
