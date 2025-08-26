-- Top 5 players by total bet in last 30 days per game_type (joins games)
with last_30 as (
  select b.player_id, g.game_type, sum(b.stake) total_bet
  from public.bets b
  join public.games g using(game_id)
  where b.bet_time >= (current_date - interval '30 day')
  group by 1,2
),
ranked as (
  select *, row_number() over(partition by game_type order by total_bet desc) rn
  from last_30
)
select * from ranked where rn <= 5 order by game_type, total_bet desc;
