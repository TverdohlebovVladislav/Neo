INSERT INTO ds.md_exchange_rate_d
    SELECT DISTINCT ON (ID, DATA_ACTUAL_DATE, CURRENCY_RK) *
    FROM temp.md_exchange_rate_d        
ON CONFLICT (ID, DATA_ACTUAL_DATE, CURRENCY_RK)
DO UPDATE 
	SET 
		DATA_ACTUAL_END_DATE = excluded.DATA_ACTUAL_END_DATE,
		REDUCED_COURCE = excluded.REDUCED_COURCE,
		CODE_ISO_NUM = excluded.CODE_ISO_NUM;