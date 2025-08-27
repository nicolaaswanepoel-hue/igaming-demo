
  create view "igaming"."public"."stg_bets__dbt_tmp"
    
    
  as (
    -- models/stg_bets.sql
with legacy as (
  -- old CSV-loaded table, if present
  select
    bet_id::text                 as bet_id,
    player_id::integer           as player_id,
    game_id::integer             as game_id,
    cast(bet_time as timestamp)  as placed_at,
    stake::double precision      as stake,
    odds::double precision       as odds,
    lower(status::text)          as status,
    actual_win::double precision as actual_win
  from public.bets
),
stream as (
  select
    bet_id,
    player_id,
    game_id,
    bet_time::timestamp          as placed_at,
    stake,
    odds,
    lower(status)                as status,
    actual_win
  from public.bets_stream
)
select * from legacy
union all
select * from stream
  );