
  create view "igaming"."public"."daily_game_metrics__dbt_tmp"
    
    
  as (
    with src as (
  select
    cast(bet_time as timestamp)               as placed_at,
    game_id::integer                          as game_id,
    player_id::integer                        as player_id,
    stake::double precision                   as stake,
    actual_win::double precision              as actual_win,
    lower(status)                             as status
  from stg_bets
)
select
  date_trunc('day', placed_at)                                 as day,
  game_id,
  count(*)                                                      as bets,
  sum(case when status = 'won'  then 1 else 0 end)             as wins,
  sum(case when status = 'lost' then 1 else 0 end)             as losses,
  sum(stake)                                                   as handle,
  sum(coalesce(stake,0) - coalesce(actual_win,0))              as ggr,
  avg(case when status='won' then 1.0 else 0.0 end)            as win_rate
from src
group by 1,2
order by 1,2
  );