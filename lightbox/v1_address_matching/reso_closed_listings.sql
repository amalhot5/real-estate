select listing_id
    , close_date
    , close_price
    , unparsed_address
    , street_number
    , street_dir_prefix
    , street_name
    -- not relevant for ICAAR
    --, street_dir_suffix
    --, street_suffix
    --, street_suffix_modifier
    , city
    , postal_code
from reso_reso_properties
where originating_system_i_d = 'icaar'
    and standard_status = 'Closed'