INSERT INTO ds.ft_posting_f
    SELECT DISTINCT ON (ID, OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK) *
    FROM temp.ft_posting_f        
ON CONFLICT (ID, OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK)
DO UPDATE 
	SET 
		CREDIT_AMOUNT = excluded.CREDIT_AMOUNT,
		DEBET_AMOUNT = excluded.DEBET_AMOUNT;