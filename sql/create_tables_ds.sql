CREATE SCHEMA IF NOT EXISTS ds;
CREATE SCHEMA IF NOT EXISTS temp;
-- DROP SCHEMA IF EXISTS ds CASCADE;

-- ft_balance_f – остатки средств на счетах;
CREATE TABLE IF NOT EXISTS ds.ft_balance_f (
    ON_DATE  DATE,
    ACCOUNT_RK NUMERIC,
    CURRENCY_RK NUMERIC,
    BALANCE_OUT DECIMAL,
    PRIMARY KEY (ON_DATE, ACCOUNT_RK)
);


-- ft_posting_f – проводки (движениясредств) по счетам;
CREATE TABLE IF NOT EXISTS ds.ft_posting_f (
    ID SERIAL,
    OPER_DATE  DATE,
    CREDIT_ACCOUNT_RK NUMERIC,
    DEBET_ACCOUNT_RK NUMERIC,
    CREDIT_AMOUNT DECIMAL,
    DEBET_AMOUNT DECIMAL,
    PRIMARY KEY (ID, OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK)
);


-- md_account_d – информация о счетах клиентов
CREATE TABLE IF NOT EXISTS ds.md_account_d (
    DATA_ACTUAL_DATE  DATE,
    DATA_ACTUAL_END_DATE DATE NOT NULL,
    ACCOUNT_RK NUMERIC,
    ACCOUNT_NUMBER VARCHAR(20)  NOT NULL,
    CHAR_TYPE VARCHAR(1)  NOT NULL,
    CURRENCY_RK NUMERIC  NOT NULL,
    CURRENCY_CODE VARCHAR(3)  NOT NULL,
    PRIMARY KEY (DATA_ACTUAL_DATE, ACCOUNT_RK)
);


-- md_currency_d – справочник валют;
CREATE TABLE IF NOT EXISTS ds.md_currency_d (
    CURRENCY_RK  NUMERIC,
    DATA_ACTUAL_DATE DATE,
    DATA_ACTUAL_END_DATE DATE,
    CURRENCY_CODE VARCHAR(3),
    CODE_ISO_CHAR VARCHAR(3),
    PRIMARY KEY (CURRENCY_RK, DATA_ACTUAL_DATE)
);


-- md_exchange_rate_d – курсы валют.
CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d (
    ID SERIAL,
    DATA_ACTUAL_DATE  DATE,
    DATA_ACTUAL_END_DATE DATE,
    CURRENCY_RK NUMERIC,
    REDUCED_COURCE DECIMAL,
    CODE_ISO_NUM VARCHAR(3),
    PRIMARY KEY (ID, DATA_ACTUAL_DATE, CURRENCY_RK)
);


-- md_ledger_account_s - справочник балансовых счётов
CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s (
    CHAPTER  VARCHAR(1),
    CHAPTER_NAME VARCHAR(16),
    SECTION_NUMBER INT,
    SECTION_NAME VARCHAR(22),
    SUBSECTION_NAME VARCHAR(21),
    LEDGER1_ACCOUNT INT,
    LEDGER1_ACCOUNT_NAME VARCHAR(47),
    LEDGER_ACCOUNT INT,
    LEDGER_ACCOUNT_NAME  VARCHAR(153),
    CHARACTERISTIC VARCHAR(1),
    IS_RESIDENT INT,
    IS_RESERVE INT,
    IS_RESERVED INT,
    IS_LOAN INT,
    IS_RESERVED_ASSETS INT,
    IS_OVERDUE INT,
    IS_INTEREST INT,
    PAIR_ACCOUNT VARCHAR(5),
    START_DATE DATE,
    END_DATE DATE,
    IS_RUB_ONLY INT,
    MIN_TERM VARCHAR(1),
    MIN_TERM_MEASURE VARCHAR(1),
    MAX_TERM VARCHAR(1),
    MAX_TERM_MEASURE VARCHAR(1),
    LEDGER_ACC_FULL_NAME_TRANSLIT VARCHAR(1),
    IS_REVALUATION VARCHAR(1),
    IS_CORRECT VARCHAR(1),
    PRIMARY KEY (LEDGER_ACCOUNT, START_DATE)
);