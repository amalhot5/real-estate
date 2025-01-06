-------------- db: euphrates-controller-prod -------------------
select source_id
    , cast(data ->> 'CloseDate' as date) as close_date
    --, cast(data ->> 'ContingentDate' as date) as contingent_date
    , cast(data ->> 'ExpirationDate' as date) as expiration_date
    , cast(data ->> 'ListingContractDate' as date) as listing_contract_date
    --, cast(data ->> 'OffMarketDate' as date) as off_market_date
    --, cast(data ->> 'OnMarketDate' as date) as on_market_date
    , cast(data ->> 'PurchaseContractDate' as date) as purchase_contract_date
    , cast(data ->> 'WithdrawnDate' as date) as withdrawn_date
    , data ->> 'ListPrice' as list_price
    , data ->> 'CurrentPrice' as current_price
    , data ->> 'OriginalListPrice' as original_list_price
    , data ->> 'ClosePrice' as close_price
    --, data ->> 'PricePerSquareFoot' as PricePerSquareFoot
    , data ->> 'PriceChangeTimestamp' as price_change_timestamp
    , data ->> 'StandardStatus' as status
    , data ->> 'PreviousListPrice' as previous_list_price
    , data ->> 'PreviousStandardStatus' as previous_status
from seen_objects 
where object_type = 'LISTING' 
    and feed_name = 'ca_crmls_rets';

------------------------ db: postgres-demo --------------------------
select id 
    , listing_id
	, listing_key
    , close_date
    , contingent_date
    , expiration_date
    , listing_contract_date
    , purchase_contract_date
    , withdrawn_date
    , list_price
	--, list_price_low
    --, current_price
    , original_list_price
    , close_price
	, price_change_timestamp
	, standard_status
	, previous_list_price
    , status_change_timestamp
    , coming_soon_timestamp
from public.reso_reso_properties 
where originating_system_i_d = 'crmls'
order by id