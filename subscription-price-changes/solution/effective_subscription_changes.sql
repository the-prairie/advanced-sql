/*
  Import necessary tables
*/

with 
  price_changes as (
      select * 
      from `advanced-sql-challenges`.`subscription_price_changes`.`subscription_price_changes`
  ),

  rebillings as (
      select * 
      from `advanced-sql-challenges`.`subscription_price_changes`.`rebillings`
  ),

  solution_check as (
      select 
        md5(
        cast(subscription_id as string) ||
        cast(new_price as string) ||
        cast(changed_at as string) ||
        cast(effective_at as string)
        ) as row_hash
      from `advanced-sql-challenges`.`subscription_price_changes`.`effective_subscription_changes`
  ),
  
/*
   Since subscriptions are updated only once a month per customer
   we only need to capture the latest price change per customer
   per month

   Using row_number() here and filtering in subsequent cte but could 
   be more elegant? clever? and just use the qualify() function in BQ
*/

  ranked_price_change as (

      select 
        *,
        row_number() over (
            partition by 
              subscription_id,
              last_day(changed_at)
            order by 
              date_diff(changed_at, last_day(changed_at) ,day) desc
        ) as price_change_rank
        
      from price_changes

  ),

  /*
    Now we can inner join the rebilling table on our ranked price changes
    making sure we only join on changes that happen on or before a billing cycle

    The inner join will drop any subscriptions that dont have any associated rebills

    We also want to calculate the nearest rebilling date for each price change
    and only keep the nearest rebill date as our effective_at 
  
  */

  calc_closest_rebill_date as (

      select 
        ranked_price_change.subscription_id,
        ranked_price_change.price as new_price,
        ranked_price_change.changed_at,
        rebillings.rebilled_at as effective_at,
        row_number() over (
            partition by 
              ranked_price_change.subscription_id,
              ranked_price_change.change_id
            order by
              date_diff(rebillings.rebilled_at, ranked_price_change.changed_at, day)
        ) as rebill_ranking
      from ranked_price_change 
      inner join rebillings on 
        ranked_price_change.subscription_id = rebillings.subscription_id and 
        ranked_price_change.changed_at <= rebillings.rebilled_at
      where 
        ranked_price_change.price_change_rank = 1
  ),
  /*
   To check our results with the solution table we can 
   create a unique hash of the row and compare it with the hash 
   of the solution row :) 
  */
  
  final as (

      select 
        subscription_id,
        new_price,
        changed_at,
        effective_at,
        md5(
          cast(subscription_id as string) ||
          cast(new_price as string) ||
          cast(changed_at as string) ||
          cast(effective_at as string)
        ) as row_hash
      from calc_closest_rebill_date 
      where 
        rebill_ranking = 1
  )

  select 
    final.*,
    case
      when solution_check.row_hash is not null then true
      else false 
    end as is_correct_solution
    
  from final
  left join solution_check on 
    final.row_hash = solution_check.row_hash

