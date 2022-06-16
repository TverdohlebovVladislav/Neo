INSERT INTO ds.md_currency_d
    SELECT DISTINCT ON (CURRENCY_RK, DATA_ACTUAL_DATE) *
    FROM temp.md_currency_d        
ON CONFLICT (CURRENCY_RK, DATA_ACTUAL_DATE)
DO UPDATE 
	SET 
		DATA_ACTUAL_END_DATE = excluded.DATA_ACTUAL_END_DATE,
		CURRENCY_CODE = excluded.CURRENCY_CODE,
		CODE_ISO_CHAR = excluded.CODE_ISO_CHAR;