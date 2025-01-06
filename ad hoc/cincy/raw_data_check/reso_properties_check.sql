select case when r.listing_id is null then 0 else 1 end as in_reso_reso_properties, count(distinct(listings.id))
FROM public.listings 
left join reso_reso_properties r
	on listings.rebny_id = r.listing_id
	
where listings.state in ('OH', 'KY', 'IN')
and listings.source in ('cincymls')
and listings.published = True
and listings.hidden in (False, NULL)
and listings.approval in (0,100)

group by 1;

select listings.id, rebny_id, list_date 
FROM public.listings 
left join reso_reso_properties r
	on listings.rebny_id = r.listing_id
	
where listings.source in ('cincymls')
and listings.published = True
and listings.hidden in (False, NULL)
and listings.approval in (0,100)
and r.listing_id is null