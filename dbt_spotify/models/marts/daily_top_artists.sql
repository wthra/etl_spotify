-- mart: top artists ranked by total stream_count per release_date
-- Pre-aggregated for DA dashboards — refreshed daily
{{
    config(
        materialized='table',
        indexes=[{'columns': ['release_date', 'rank_by_streams'], 'type': 'btree'}]
    )
}}

with base as (
    select
        f.release_date,
        a.artist_name,
        a.artist_id,
        sum(f.stream_count)         as total_streams,
        avg(f.popularity)::numeric(5,2) as avg_popularity,
        count(distinct f.track_id)  as track_count,
        count(distinct f.country)   as country_reach
    from {{ ref('fact_streams') }}  f
    join {{ ref('dim_artist') }}    a using (artist_id)
    group by 1, 2, 3
),

ranked as (
    select
        *,
        rank() over (
            partition by release_date
            order by total_streams desc
        ) as rank_by_streams
    from base
)

select * from ranked
