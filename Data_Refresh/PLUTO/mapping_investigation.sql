select distinct neighborhood_id
from buildings b
inner join building_geographies bg on b.id = bg.building_id
where bg.geography_id = 1278
order by 1 desc;

