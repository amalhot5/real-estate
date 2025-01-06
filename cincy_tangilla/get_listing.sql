SELECT DISTINCT (listing_agents.first_name || ' ' || listing_agents.last_name) AS full_name, 
count(*)
FROM listings
LEFT JOIN listing_agents ON listing_agents.listing_id = listings.id
WHERE (listing_agents.first_name || ' ' || listing_agents.last_name) in ('Susan A Keefer')
and listings.office_id is not null
and listings.source = 'cincymls'
group by (listing_agents.first_name || ' ' || listing_agents.last_name)