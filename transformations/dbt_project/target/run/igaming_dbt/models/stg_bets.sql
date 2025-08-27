
  create view "igaming"."public"."stg_bets__dbt_tmp"
    
    
  as (
    with src as (
  select bet_id, player_id, game_id, stake, odds, bet_time, status, actual_win
  from public.bets
)
select * from src
  );