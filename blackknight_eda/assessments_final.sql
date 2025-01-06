-------------------------- dupe units ----------------------------
with dupe_addresses as (
	SELECT PropertyFullStreetAddress
		, PropertyZipCode
		, AssessmentYear
		, count(*) as assessment_count
	from Assessment a
	where AssessmentYear = 2023
	group by PropertyFullStreetAddress
		, PropertyZipCode
		, AssessmentYear
	having count(*) > 1
), dupe_units as (
	SELECT a.PropertyFullStreetAddress
		, COALESCE(a.PropertyUnitNumber, 'na') as PropertyUnitNumber
		, a.PropertyZipCode
		, a.AssessmentYear
		, count(*) as assessment_count
	from Assessment a
		inner join dupe_addresses 
			on dupe_addresses.PropertyFullStreetAddress = a.PropertyFullStreetAddress 
			and a.PropertyZipCode = dupe_addresses.PropertyZipCode 
			and a.AssessmentYear = dupe_addresses.AssessmentYear
	group by a.PropertyFullStreetAddress
		, COALESCE(a.PropertyUnitNumber, a.[Legal:LotNumber], 'na')
		, a.PropertyZipCode
		, a.AssessmentYear
	having count(*) > 1
)
--SELECT COUNT(*) from dupe_units; 
SELECT a.*
--select a.[CountyLand-UseDescription], count(*)
from Assessment a
	inner join dupe_units 
		on dupe_units.PropertyFullStreetAddress = a.PropertyFullStreetAddress 
		AND dupe_units.PropertyZipCode = a.PropertyZipCode
		and dupe_units.PropertyUnitNumber = COALESCE(a.PropertyUnitNumber, a.[Legal:LotNumber], 'na')
		and dupe_units.AssessmentYear = a.AssessmentYear
--group by a.[CountyLand-UseDescription];
		
----------------- unique address, unique units --------------------------
with unique_addresses as (
	SELECT PropertyFullStreetAddress
		, PropertyCityName 
		, PropertyZipCode
		, AssessmentYear
		, count(*) as assessment_count
	from Assessment a
	where AssessmentYear = 2023
	group by PropertyFullStreetAddress
		, PropertyCityName 
		, PropertyZipCode
		, AssessmentYear
	having count(*) = 1
)
SELECT count(*)
from Assessment a
	inner join unique_addresses 
		on unique_addresses.PropertyFullStreetAddress = a.PropertyFullStreetAddress 
		AND unique_addresses.PropertyZipCode = a.PropertyZipCode 
		and unique_addresses.PropertyCityName = a.PropertyCityName 
		and a.AssessmentYear = unique_addresses.AssessmentYear
--where not PropertyUnitNumber is null 
;
------------------------------ dupe address, unique unit ---------------------------------------
with dupe_addresses as (
	SELECT PropertyFullStreetAddress
		, PropertyZipCode
		, AssessmentYear
		, count(*) as assessment_count
	from Assessment a
	where AssessmentYear = 2023
	group by PropertyFullStreetAddress
		, PropertyZipCode
		, AssessmentYear
	having count(*) > 1
), unique_units as (
	SELECT a.PropertyFullStreetAddress
		, COALESCE(a.PropertyUnitNumber, 'na') as PropertyUnitNumber
		, a.PropertyZipCode
		, a.AssessmentYear
		, count(*) as assessment_count
	from Assessment a
		inner join dupe_addresses 
			on dupe_addresses.PropertyFullStreetAddress = a.PropertyFullStreetAddress 
			and a.PropertyZipCode = dupe_addresses.PropertyZipCode 
			and a.AssessmentYear = dupe_addresses.AssessmentYear
	group by a.PropertyFullStreetAddress
		, COALESCE(a.PropertyUnitNumber, 'na')
		, a.PropertyZipCode
		, a.AssessmentYear
	having count(*) = 1
)
SELECT a.*
from Assessment a
	inner join unique_units 
		on unique_units.PropertyFullStreetAddress = a.PropertyFullStreetAddress 
		AND unique_units.PropertyZipCode = a.PropertyZipCode
		and unique_units.PropertyUnitNumber = COALESCE(a.PropertyUnitNumber, a.[Legal:LotNumber], 'na')
		and unique_units.AssessmentYear = a.AssessmentYear
;

--------- dupe units with building record --------------
with unique_units as (
	SELECT PropertyFullStreetAddress
		, PropertyUnitNumber 
		, PropertyZipCode
		, AssessmentYear
		, count(*) as assessment_count
	from Assessment a
	where AssessmentYear = 2023
	group by PropertyFullStreetAddress
		, PropertyUnitNumber 
		, PropertyZipCode
		, AssessmentYear
	having count(*) = 1
), dupe_units as (
	SELECT PropertyFullStreetAddress 
		, PropertyUnitNumber
		, PropertyZipCode
		, AssessmentYear
		, count(*) as assessment_count
	from Assessment a
	where AssessmentYear = 2023
	group by PropertyFullStreetAddress
		, PropertyUnitNumber
		, PropertyZipCode
		, AssessmentYear
	having count(*) > 1
)
SELECT b.*
from unique_units a
	inner join dupe_units b on a.PropertyFullStreetAddress = b.PropertyFullStreetAddress 
	and a.PropertyZipCode = b.PropertyZipCode 
	--and a.PropertyUnitNumber = b.PropertyUnitNumber
--where b.PropertyFullStreetAddress is null
;