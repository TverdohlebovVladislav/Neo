create or replace FUNCTION get_max_min_credit_debit_from_date (date_max_min date)
	returns table (
        "Date" date, 
        "MAX_CREDIT" numeric,
        "MIN_CREDIT" numeric,
        "MAX_DEBET" numeric,
        "MIN_DEBET" numeric
    )
	language sql as
	$$
		SELECT 
			OPER_DATE as "Date",
            MAX(CREDIT_AMOUNT) AS "MAX_CREDIT",
            MIN(CREDIT_AMOUNT) AS "MIN_CREDIT",
            MAX(DEBET_AMOUNT) AS "MAX_DEBET",
            MIN(DEBET_AMOUNT) AS "MIN_DEBET"
		FROM ds.ft_posting_f
		WHERE OPER_DATE = date_max_min
		GROUP BY "Date";
	$$;
	
-- Вызов функции
-- SELECT * from get_max_min_credit_debit_from_date('2018-01-18'::date);