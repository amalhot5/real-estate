select distinct(b.id) from buildingdir.map_pluto_v17_1 p
inner join buildings b
    on b.source_id = p.bbl
    and nullif(zip, '')::int = zipcode
    and year_built = yearbuilt
    and num_stories = numfloors 
    and num_units = unitsres
    and lot_area = lotarea
    and lot_front = lotfront
    and lot_depth = lotdepth
    and building_class = bldgclass
    and building_front = bldgfront
    and building_depth = bldgdepth
    and building_area = bldgarea