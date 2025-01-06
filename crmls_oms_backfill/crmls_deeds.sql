SELECT 
	--case when propertyhousenumber is null then false else true end as has_house_number
	--, standardizedlanduse
	--, count(*)
	dpid
	, recordingdate
	, salesprice
	, standardizedlanduse
	, propertyfullstreetaddress
	, propertycityname
	, propertyzipcode
	, propertyhousenumber
	, propertyunitnumber
	, seller1firstnamemiddlename
	, seller1lastnameorcorporationname
	, buyer1firstnamemiddlename
	, buyer1lastnameorcorporationname
	
FROM crmls_bk_062024.raw_deeds
where salesprice != '0.0' 
	and salesprice != '0'
	and not salesprice is null
	and not recordingdate is null
	and not propertyfullstreetaddress is null
--group by 1,2
order by dpid
offset 0
limit 5000000

-- 36,338,014 rows with zero included
-- 14,544,114 rows without zero

select * from crmls_bk_062024.raw_deeds where dpid in ('060730167574', '060730167575', '060730167576') order by recordingdate, salesprice


select propertyfullstreetaddress
	, propertycityname
	, propertyzipcode
	, recordingdate
	, seller1lastnameorcorporationname
	, buyer1lastnameorcorporationname
	, salesprice
	, count(*)
from crmls_bk_062024.raw_deeds
where salesprice != '0.0' 
	and salesprice != '0'
	and not salesprice is null
	and not recordingdate is null
	and not propertyfullstreetaddress is null
group by propertyfullstreetaddress
	, propertycityname
	, propertyzipcode
	, recordingdate
	, seller1lastnameorcorporationname
	, buyer1lastnameorcorporationname
	, salesprice
having count(*)>1
order by propertyfullstreetaddress desc