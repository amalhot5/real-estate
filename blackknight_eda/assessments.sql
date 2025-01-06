with cte as (
	SELECT PropertyFullStreetAddress
		, count(*) as assessment_count
	from Assessment a
	group by PropertyFullStreetAddress 
	having count(*)=1
)
select a.DPID, a.PropertyFullStreetAddress 
	--count(*)
from Assessment a
	inner join cte on cte.propertyfullstreetaddress = a.PropertyFullStreetAddress
--where a.NumberofUnits > 1
--limit 1000
;

with cte as (
	select hashbytes('MD5', p.ogr_geometry.STAsText()) as hashed_geo, count(*) as parcel_count
	from parcels p 
	group by hashbytes('MD5', p.ogr_geometry.STAsText())
	having count(*)>1
)
select * from parcels p 
	inner join cte on hashed_geo=hashbytes('MD5', p.ogr_geometry.STAsText())
--here parcel_count=1
;

select * from parcels p where dpid ='060710160630';

---------------------- Assessments one address one building -------------------------
with cte as (
	SELECT PropertyFullStreetAddress
		, count(*) as assessment_count
	from Assessment a
	group by PropertyFullStreetAddress 
	having count(*)=1
)
select a.DPID, a.PropertyFullStreetAddress 
from Assessment a
	inner join cte on cte.propertyfullstreetaddress = a.PropertyFullStreetAddress
--where a.NumberofUnits <= 1
;

---------------------- Assessments one address one unit, many dupes -------------------------
with cte as (
	select a.PropertyFullStreetAddress
		, COALESCE(a.propertyunitnumber, a.[Legal:LotNumber]) as unit_number
		, count(distinct(dpid)) as dpid_count
	from Assessment a 
	group by a.PropertyFullStreetAddress, COALESCE(a.propertyunitnumber, a.[Legal:LotNumber]) 
	having count(distinct(dpid)) = 1
)
select a.*
from Assessment a 
	inner join cte 
		on cte.PropertyFullStreetAddress = a.PropertyFullStreetAddress and COALESCE(a.propertyunitnumber, a.[Legal:LotNumber]) = unit_number
;

---------------------- Assessments one address many units -------------------------
with cte as (
	select a.PropertyFullStreetAddress
		, count(distinct(dpid)) as dpid_count
	from Assessment a 
	--where not COALESCE(a.propertyunitnumber, a.[Legal:LotNumber]) is null
	group by a.PropertyFullStreetAddress
	having count(distinct(dpid)) > 1
)
select --a.*
	a.DPID 
	, a.PropertyFullStreetAddress
	, a.PropertyUnitNumber 
	, a.[Legal:LotNumber]
	, a.[Legal:BriefDescription]
from Assessment a 
	inner join cte 
		on cte.PropertyFullStreetAddress = a.PropertyFullStreetAddress 

;

select count(*) from Assessment a 