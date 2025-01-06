select * from parcels p --inner join Assessment a on a.APN = p.parcelapn and a.FIPSCode=p.fips
where p.dpid is null;

with cte as (select hashbytes('MD5', p.ogr_geometry.STAsText()) as hashed_geo, count(*) as parcel_count
from parcels p 
group by hashbytes('MD5', p.ogr_geometry.STAsText())
having count(*)=1
)
select count(*) from cte-- parcels p 
	--inner join cte on hashed_geo=hashbytes('MD5', p.ogr_geometry.STAsText())
--here parcel_count=1
;