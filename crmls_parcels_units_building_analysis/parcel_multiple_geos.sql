select i."ParcelFullAddress", "GeoJson_Polygon", ogr_fid, parcelapn, count_geom
from crmls_bk.irvine i 
inner join (
    select "ParcelFullAddress" , count(distinct("GeoJson_Polygon")) as count_geom
    from crmls_bk.irvine i
    group by "ParcelFullAddress"
    having count(distinct("GeoJson_Polygon")) > 1
    ) x 
    on x."ParcelFullAddress" = i."ParcelFullAddress" 
where i."ParcelFullAddress" != '      ' 
order by count_geom desc;