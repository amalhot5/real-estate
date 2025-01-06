select dpid, count(*)
from dbo.Assessment a 
group by dpid
having count(*) > 1;

select distinct AssessmentYear
from dbo.Assessment a ;

select dpid, count(*)
from dbo.Assessment a 
group by dpid 
having count(*)>1;


select dpid, count(distinct(rsthsnum + rststname + rstsuffix + rstzip)), count(*)
from dbo.parcels p 
where not rststname is NULL 
group by dpid 
HAVING count(*)>1;

select * from parcels where dpid = '060010019248';


select PropertyFullStreetAddress, PropertyUnitType, PropertyUnitNumber, [Legal:LotNumber] , [PropertyAddress:Latitude], [PropertyAddress:Longitude] from Assessment a where dpid in ('060710357179','060710357191','060710357195');

select * from parcels where dpid = '060591170725';


select MainBuildingAreaIndicator, count(*)
from Assessment a 
group by MainBuildingAreaIndicator;


select count(*) 
from Assessment a 
where NumberofUnits != 0;

select FIPSCode, APN, count(distinct(dpid)) from Assessment a 
group by FIPSCode, APN 
having count(distinct(dpid)) > 1;


with cte as (select p.dpid, count(*) as parcel_count
from parcels p 
group by dpid 
having count(*) > 1)
select count(*) from cte;

select count(*) from parcels p ;


SELECT xCentroid_geometry, xCentroid_geometry.STY as centroid_latitude, xCentroid_geometry.STX as centroid_longitude, ogr_geometry,

'{' +
(CASE ogr_geometry.STGeometryType()
WHEN 'POINT' THEN
'"type": "Point","coordinates":' +
REPLACE(REPLACE(REPLACE(REPLACE(ogr_geometry.ToString(),'POINT ',''),'(','['),')',']'),' ',',')
WHEN 'POLYGON' THEN
'"type": "Polygon","coordinates":' +
'[' + REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ogr_geometry.ToString(),'POLYGON ',''),'(','['),')',']'),'], ',']],['),', ','],['),' ',',') + ']'
WHEN 'MULTIPOLYGON' THEN
'"type": "MultiPolygon","coordinates":' +
'[' + REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ogr_geometry.ToString(),'MULTIPOLYGON ',''),'(','['),')',']'),'], ',']],['),', ','],['),' ',',') + ']'
WHEN 'MULTILINESTRING' THEN
'"type": "MultiLineString","coordinates":' +
'[' + REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ogr_geometry.ToString(),'MULTILINESTRING ',''),'(','['),')',']'),'], ',']],['),', ','],['),' ',',') + ']'
WHEN 'MULTIPOINT' THEN
'"type": "MultiPoint","coordinates":' +
REPLACE( REPLACE(REPLACE(REPLACE(REPLACE(ogr_geometry.ToString(),'MULTIPOINT ',''),'(','['),')',']'),' ',','),',,',', ')
WHEN 'LINESTRING' THEN
'"type": "LineString","coordinates":' +
'[' + REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ogr_geometry.ToString(),'LINESTRING ',''),'(','['),')',']'),'], ',']],['),', ','],['),' ',',') + ']'
ELSE NULL
END)
+'}' as geom,

 * FROM dbo.Parcels

where DPID = '060010023098'
--where dpid in ('060710357179','060710357191','060710357195')
;


select * from parcels p1 inner join parcels p2 on p1.id != p2.id and p2.ogr_geometry.STEquals(p1.ogr_geometry) = 1
;

select p.Id
	, p.parcelapn 
	, p.taxapn 
	, p.fips 
	, p.sthsnum 
	, p.stdir 
	, p.ststname 
	, p.stsuffix
	, p.stquadrant 
	, p.stunitprfx 
	, p.stunitnum 
	, p.stcity 
	, p.ststate 
	, p.stzip 
	, p.dpid 
	, p.parcelid 
	, p.ogr_geometry.STAsText()
from parcels p 
;
with cte as (select a.PropertyFullStreetAddress, COALESCE(a.propertyunitnumber, a.[Legal:LotNumber]) as unit_number
	, count(distinct(dpid)) as dpid_count
from Assessment a 
group by a.PropertyFullStreetAddress, COALESCE(a.propertyunitnumber, a.[Legal:LotNumber]) 
having count(distinct(dpid)) > 1
)
select count(*) from Assessment a 
	inner join cte 
		on cte.PropertyFullStreetAddress = a.PropertyFullStreetAddress and COALESCE(a.propertyunitnumber, a.[Legal:LotNumber]) = unit_number
;

with cte as (select a.PropertyFullStreetAddress, COALESCE(a.propertyunitnumber, a.[Legal:LotNumber]) as unit_number
	, count(distinct(dpid)) as dpid_count
from Assessment a 
group by a.PropertyFullStreetAddress, COALESCE(a.propertyunitnumber, a.[Legal:LotNumber]) 
)
select count(*) 
from cte
where dpid_count = 1;

SELECT  
from Assessment a 


select a.PropertyFullStreetAddress, a.PropertyUnitType , a.PropertyUnitNumber 
from Assessment a 
where not a.PropertyUnitNumber is null;




select count(*) from Assessment a ;


with cte as (select hashbytes('MD5', p.ogr_geometry.STAsText()) as hashed_geo, count(*) as parcel_count
from parcels p 
group by hashbytes('MD5', p.ogr_geometry.STAsText())
having count(*)>1
)
select count(*) from parcels p 
	inner join cte on hashed_geo=hashbytes('MD5', p.ogr_geometry.STAsText())
--here parcel_count=1
;

select count(*) from parcels p where dpid is null
;

select a.id, a.PropertyFullStreetAddress 
	, a.PropertyUnitNumber 
	, a.PropertyCityName 
	, a.PropertyZipCode 
	, p.Id 
	, p.sthsnum 
	, p.stdir 
	, p.ststname 
	, p.stunitprfx 
	, p.stunitnum 
	, p.stcity 
	, p.ststate 
	, p.stzip 
from Assessment a 
	inner join parcels p on a.DPID = p.dpid 
where p.sthsnum != a.PropertyHouseNumber 
	and p.ststname != a.PropertyStreetName 
	and p.stsuffix != a.PropertyStreetSuffix 
	and p.stunitnum != a.PropertyUnitNumber 
	and p.stzip != a.PropertyZipCode ;
	
select * from Assessment a where dpid='060850702544' --PropertyFullStreetAddress = '70 W HEDDING ST'
;

select * from parcels p where dpid = '060850702544';


select count(*) from parcels p where dpid is null;

select count(*) from Assessment a 
	right join parcels p on a.DPID = p.dpid 
where a.id is null and not p.dpid is null
;

select count(*)
from parcels
--where not parcels.dpid in (select dpid from assessment)
--	or parcels.dpid is null