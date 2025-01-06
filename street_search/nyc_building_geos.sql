select b.id, b.display_address, st_asgeojson(b.geometry) as geometry
from buildings b
inner join building_geographies bg on bg.geography_id=1278 and bg.building_id = b.id
where b.in_search=true;