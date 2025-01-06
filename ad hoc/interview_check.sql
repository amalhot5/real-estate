with current_status as (
    select listing_id
        , price
        , status_code
        , update_transaction
        , row_number() over (partition by listing_id order by update_transaction desc) as rank
    from listing_histories
)

select id
    , address
    , status_code as last_status
    , price as last_price
    , year_built
    , building_type
from listings 
    join current_status on id = listing_id and rank=1
    left join buildings on building_id = id
where listings.state='NY'