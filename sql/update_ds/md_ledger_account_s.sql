INSERT INTO ds.md_ledger_account_s
    SELECT DISTINCT ON (LEDGER_ACCOUNT, START_DATE) *
    FROM temp.md_ledger_account_s        
ON CONFLICT (LEDGER_ACCOUNT, START_DATE)
DO UPDATE 
	SET 
		CHAPTER = excluded.CHAPTER,
		CHAPTER_NAME = excluded.CHAPTER_NAME,
        SECTION_NUMBER = excluded.SECTION_NUMBER,
		SECTION_NAME = excluded.SECTION_NAME,
        SUBSECTION_NAME = excluded.SUBSECTION_NAME,
		LEDGER1_ACCOUNT = excluded.LEDGER1_ACCOUNT,
        LEDGER1_ACCOUNT_NAME = excluded.LEDGER1_ACCOUNT_NAME,
		LEDGER_ACCOUNT_NAME = excluded.LEDGER_ACCOUNT_NAME,
        CHARACTERISTIC = excluded.CHARACTERISTIC,
		IS_RESIDENT = excluded.IS_RESIDENT,
        IS_RESERVE = excluded.IS_RESERVE,
		IS_RESERVED = excluded.IS_RESERVED,
        IS_LOAN = excluded.IS_LOAN,
		IS_RESERVED_ASSETS = excluded.IS_RESERVED_ASSETS,
        IS_OVERDUE = excluded.IS_OVERDUE,
		IS_INTEREST = excluded.IS_INTEREST,
        PAIR_ACCOUNT = excluded.PAIR_ACCOUNT,
		END_DATE = excluded.END_DATE,
        IS_RUB_ONLY = excluded.IS_RUB_ONLY,
		MIN_TERM = excluded.MIN_TERM,
        MIN_TERM_MEASURE = excluded.MIN_TERM_MEASURE,
		MAX_TERM = excluded.MAX_TERM,
        MAX_TERM_MEASURE = excluded.MAX_TERM_MEASURE,
		LEDGER_ACC_FULL_NAME_TRANSLIT = excluded.LEDGER_ACC_FULL_NAME_TRANSLIT,
        IS_REVALUATION = excluded.IS_REVALUATION,
		IS_CORRECT = excluded.IS_CORRECT;