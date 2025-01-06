select am.* from (
    select b.*
    from buildings b
    ---join with geographies
    inner join building_geographies bg
        on b.id = bg.building_id
    left join (
        select
            building_id
            , count(*) as num_other_listings
            , max(created_at) as last_listing
        from listings
        where not(
                lower(source) like '%mlsli%' 
                or lower(source) like '%rls%' 
                or source = 'real_plus')
        group by building_id
    ) l on l.building_id = b.id
    left join (
        select
            building_id
            , count(distinct(id)) as num_rls
            , max(created_at) as last_rls_listing
        from listings
        where --not(lower(source) like '%mlsli%')
            lower(source) like '%rls%' or source = 'real_plus'
        group by building_id
    ) rls on rls.building_id = b.id
    left join (
        select
            building_id
            , count(distinct(id)) as num_mlsli
            , max(created_at) as last_mlsli_listing
        from listings
        where lower(source) like '%mlsli%'
        group by building_id
    ) mlsli on mlsli.building_id = b.id
    left join (
        select 
            building_id
            , count(distinct(acris_document_id)) as num_acris
            , max(created_at) as last_acris_created
            , max(sale_date) as last_acris_sale
        from historicals
        group by building_id
        order by count(distinct(acris_document_id)) desc
    ) a on b.id = a.building_id
    where bg.geography_id = 1278
) bu 
inner join amenities am
    on bu.id = am.amenities_id
where amenities_type = 'Building'