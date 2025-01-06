with max_listing_history as (
select listing_id
    , status_code
    , update_transaction
    , row_number() over(partition by listing_id order by update_transaction desc) as rank
from listing_histories
)
select listing_id
    , r.standard_status
    , mlh.status_code
    , r.listing_id
    , r.cancellation_date
    , r.close_date
    , r.contingent_date
    , r.expiration_date
    , r.listing_contract_date
    , r.off_market_date
    , r.purchase_contract_date
    , r.withdrawn_date
from reso_reso_properties r
	join listings l
		on l.rebny_id = r.listing_id
	join max_listing_history mlh
		on mlh.listing_id = l.id
where r.originating_system_i_d = 'crmls'
	and (
        not(status_code = 100 and standard_status = 'Active') or
        not(status_code in (400, 500) and standard_status = 'Closed') or
        not(contingent_date = 200 and standard_status = 'Contingent') or
        not(status_code = 240 and standard_status in ('Pending', 'Active Under Contract')) or
        not(status_code = 300 and standard_status = 'Expired') or
        not(status_code = 999 and standard_status = 'Incomplete') or
        not(status_code = 640 and standard_status = 'Hold') or
        not(status_code = 600 and standard_status = 'Withdrawn') or
        not(status_code = 620 and standard_status = 'Canceled') or
        not(status_code = 600 and standard_status = 'Delete') 
    )