select o.name, count(distinct(la.agent_id)) as member_count, count(distinct(l.id)) as listing_count
from listings l
left join listing_agents la
    on la.listing_id = l.id
inner join brokerages br 
    on l.brokerage_id = br.id
inner join offices o
    on o.brokerage_id = br.id
where o.name in ('The A.W. Gerdsen Company, LLC',
'RE/MAX Acclaimed Realty, Inc.',
'Western & Southern Financial G',
'Tartan Property, Inc.',
'Wallick Properties Midwest LLC',
'RE/MAX P&O Company',
'Koogler Realtors GMAC',
'US Bank Corporate Real Estate',
'Ohio National Financial Servic',
'One Percent Realty',
'Society Real Estate',
'Real Estate Mortgage Corp.',
'Coldwell Banker Realty',
'Union Savings Bank',
'HER Realtors',
'%Council of REALTORS',
'InFocus Real Estate Group LLC',
'Bennett Realty',
'Mike Robinette, Broker',
'First Community Mortgage',
'Rebuilt Realty',
'Align Right Realty Infinity',
'Home Mart',
'Howard Hanna Real Estate Svcs',
'Howard Hanna Real Estate Svcs',
'Carrington RealEstate Services',
'Keller Williams Advantage Real',
'Bridgestream Realty',
'Re/Max 100, Inc.',
'Equity Resources',
'Equity Resources',
'Fifth Third Bank',
'ReMax on the Move',
'Cincinnati Federal',
'Bauer Real Estate Company',
'MICHAEL COMBS INC',
'HER Realtors',
'Charter Oak Realty',
'Tracker Realty Group, Inc.',
'Howard Hanna Real Estate Svcs',
'RE/MAX Victory + Affiliates',
'X Real Estate',
'HomeByte',
'Southwest Ohio TenantRep, LLC')
group by 1;