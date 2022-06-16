
-- ---
-- ds.ft_balance_f TABLE from temp schema
INSERT INTO ds.ft_balance_f
    SELECT *
    FROM temp.ft_balance_f        
ON CONFLICT (ON_DATE ,ACCOUNT_RK)
DO UPDATE 
	SET 
		CURRENCY_RK = excluded.CURRENCY_RK,
		BALANCE_OUT = excluded.BALANCE_OUT;