SELECT dpid 
	, recordingdate
	, salesprice
	, standardizedlanduse
FROM crmls_bk_062024.raw_deeds
where salesprice != '0.0';

-- 36,338,014 rows with zero included
-- 29,748,184 rows without zero