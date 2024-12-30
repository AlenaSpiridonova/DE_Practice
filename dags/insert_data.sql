-- 1. Создание схемы DS для детального слоя данных
CREATE USER ds_user WITH PASSWORD 'ds_password';
CREATE SCHEMA IF NOT EXISTS DS AUTHORIZATION ds_user;
ALTER ROLE ds_user SET search_path TO DS;
GRANT USAGE ON SCHEMA DS TO ds_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA DS TO ds_user;
GRANT CREATE ON SCHEMA DS TO ds_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA DS TO ds_user;


--. Создание в схеме DS таблиц для данных из CSV-файлов
CREATE TABLE DS.FT_BALANCE_F (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    currency_rk NUMERIC,
    balance_out FLOAT,
    PRIMARY KEY (on_date, account_rk)
);

CREATE TABLE DS.FT_POSTING_F (
    oper_date DATE NOT NULL,
    credit_account_rk NUMERIC NOT NULL,
    debet_account_rk NUMERIC NOT NULL,
    credit_amount FLOAT,
    debet_amount FLOAT
);

CREATE TABLE DS.MD_ACCOUNT_D (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    char_type VARCHAR(1) NOT NULL,
    currency_rk NUMERIC NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    PRIMARY KEY (data_actual_date, account_rk)
);

CREATE TABLE DS.MD_CURRENCY_D (
    currency_rk NUMERIC NOT NULL,
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_code VARCHAR(3),
    code_iso_char VARCHAR(3),
    PRIMARY KEY (currency_rk, data_actual_date)
);

CREATE TABLE DS.MD_EXCHANGE_RATE_D (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_rk NUMERIC NOT NULL,
    reduced_cource FLOAT,
    code_iso_num VARCHAR(3),
    PRIMARY KEY (data_actual_date, currency_rk)
);

CREATE TABLE DS.MD_LEDGER_ACCOUNT_S (
    chapter CHAR(1),
    chapter_name VARCHAR(16),
    section_number INTEGER,
    section_name VARCHAR(22),
    subsection_name VARCHAR(21),
    ledger1_account INTEGER,
    ledger1_account_name VARCHAR(47),
    ledger_account INTEGER NOT NULL,
    ledger_account_name VARCHAR(153),
    characteristic CHAR(1),
    is_resident INTEGER,
    is_reserve INTEGER,
    is_reserved INTEGER,
    is_loan INTEGER,
    is_reserved_assets INTEGER,
    is_overdue INTEGER,
    is_interest INTEGER,
    pair_account VARCHAR(5),
    start_date DATE NOT NULL,
    end_date DATE,
    is_rub_only INTEGER,
    min_term VARCHAR(1),
    min_term_measure VARCHAR(1),
    max_term VARCHAR(1),
    max_term_measure VARCHAR(1),
    ledger_acc_full_name_translit VARCHAR(1),
    is_revaluation VARCHAR(1),
    is_correct VARCHAR(1),
    PRIMARY KEY (ledger_account, start_date)
);


-- 2. Создание схемы LOGS для хранения логов
CREATE USER logs_user WITH PASSWORD 'logs_password';
CREATE SCHEMA IF NOT EXISTS LOGS AUTHORIZATION logs_user;
ALTER ROLE logs_user SET search_path TO LOGS;
GRANT USAGE ON SCHEMA LOGS TO logs_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA LOGS TO logs_user;
GRANT CREATE ON SCHEMA LOGS TO logs_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA LOGS TO logs_user;


-- Создание в схеме LOGS таблицы для логирования ETL-процессов
CREATE TABLE LOGS.etl_logs (
    log_id SERIAL PRIMARY KEY,
    process VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(50),
    message TEXT,
    input_rows INTEGER DEFAULT 0,
    inserted_rows INTEGER DEFAULT 0,
    updated_rows INTEGER DEFAULT 0,
    skipped_rows INTEGER DEFAULT 0
);


-- 3. Логирование начала ETL-процесса
INSERT INTO LOGS.etl_logs (process_name, start_time, status, message)
VALUES ('CSV_to_DS_Load', NOW(), 'STARTED', 'Начало загрузки данных из CSV в DS.');

-- Пауза для визуализации времени выполнения
DO $$ BEGIN
    PERFORM pg_sleep(5);
END $$;

-- Логирование завершения ETL-процесса
UPDATE LOGS.etl_logs
SET end_time = NOW(), status = 'COMPLETED', message = 'Загрузка данных завершена успешно.'
WHERE process_name = 'CSV_to_DS_Load' AND status = 'STARTED';




CREATE OR REPLACE FUNCTION PROCESS_FT_BALANCE_F(
) RETURNS TABLE(input_count INTEGER, inserted_count INTEGER, updated_count INTEGER, skipped_count INTEGER, message_for_log TEXT) AS $$
DECLARE
    input_count INTEGER := 0;
    inserted_count INTEGER := 0;
	before_insert_count INTEGER := 0;
    updated_count INTEGER := 0;
	skipped_count INTEGER := 0;
BEGIN
    -- Считаем полученные из csv строки в таблице stage
    input_count := (SELECT COUNT(*) FROM stage.FT_BALANCE_F);

    -- Сначала обновляем строки, которые уже существуют в DS
    WITH updated_rows AS (
  		UPDATE DS.FT_BALANCE_F d
        SET
            currency_rk = CAST(s."CURRENCY_RK" AS NUMERIC),
            balance_out = CAST(s."BALANCE_OUT" AS FLOAT)
        FROM stage.FT_BALANCE_F s
        WHERE 
			d.on_date = TO_DATE(s."ON_DATE", 'DD.MM.YYYY') 
          	AND d.account_rk = s."ACCOUNT_RK"
          	AND (
	            d.currency_rk IS DISTINCT FROM CAST(s."CURRENCY_RK" AS NUMERIC) OR
				d.balance_out IS DISTINCT FROM CAST(s."BALANCE_OUT" AS FLOAT)
	        )
        RETURNING d.on_date, d.account_rk
    )
    -- Подсчитываем количество обновлений
    SELECT COUNT(*) INTO updated_count FROM updated_rows;

	before_insert_count := (SELECT COUNT(*) FROM DS.FT_BALANCE_F);

    -- Затем вставляем в DS новые строки, которых нет в этой схеме
    INSERT INTO DS.FT_BALANCE_F (on_date, account_rk, currency_rk, balance_out)
    SELECT 
		TO_DATE(s."ON_DATE", 'DD.MM.YYYY'),
		CAST(s."ACCOUNT_RK" AS NUMERIC),
		CAST(s."CURRENCY_RK" AS NUMERIC),
		CAST(s."BALANCE_OUT" AS FLOAT)
    FROM (SELECT DISTINCT * FROM stage.FT_BALANCE_F) s
    LEFT JOIN DS.FT_BALANCE_F d ON d.on_date = TO_DATE(s."ON_DATE", 'DD.MM.YYYY') AND d.account_rk = s."ACCOUNT_RK"
    WHERE d.on_date IS NULL
	ON CONFLICT DO NOTHING;

    -- Подсчитываем количество вставленных строк
    inserted_count := (SELECT COUNT(*) FROM DS.FT_BALANCE_F) - before_insert_count;

	skipped_count := input_count - inserted_count - updated_count;

	-- Сообщение для лога
	SELECT STRING_AGG(
	    FORMAT(
	        'Несоответствие данных - on_date: %s, account_rk: %s',
	        t."ON_DATE", t."ACCOUNT_RK"
	    ),
	    '; '
	)
	INTO message_for_log
	FROM (
	    SELECT 
	        s."ON_DATE", 
	        s."ACCOUNT_RK", 
	        s."CURRENCY_RK", 
	        s."BALANCE_OUT"
	    FROM stage.FT_BALANCE_F s
	    LEFT JOIN DS.FT_BALANCE_F d ON d.on_date = TO_DATE(s."ON_DATE", 'DD.MM.YYYY') AND d.account_rk = s."ACCOUNT_RK"
	    WHERE
	        d.on_date IS NULL
	        OR (
	            d.currency_rk IS DISTINCT FROM CAST(s."CURRENCY_RK" AS NUMERIC) OR
	            d.balance_out IS DISTINCT FROM CAST(s."BALANCE_OUT" AS FLOAT)
	        )
	) t;

    -- Возвращаем статистику
    RETURN QUERY SELECT input_count, inserted_count, updated_count, skipped_count, message_for_log;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION PROCESS_FT_POSTING_F(
) RETURNS TABLE(input_count INTEGER, inserted_count INTEGER, updated_count INTEGER, skipped_count INTEGER, message_for_log TEXT) AS $$
DECLARE
    input_count INTEGER := 0;
    inserted_count INTEGER := 0;
	before_insert_count INTEGER := 0;
    updated_count INTEGER := 0;
	skipped_count INTEGER := 0;
BEGIN
    -- Считаем полученные из csv строки в таблице stage
    input_count := (SELECT COUNT(*) FROM stage.FT_POSTING_F);

   TRUNCATE TABLE DS.FT_POSTING_F;

    -- Затем вставляем в DS новые строки, которых нет в э  ой схеме
    INSERT INTO DS.FT_POSTING_F (oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount)
    SELECT
	    CAST(s."OPER_DATE" AS DATE),
		CAST(s."CREDIT_ACCOUNT_RK" AS NUMERIC),
		CAST(s."DEBET_ACCOUNT_RK" AS NUMERIC),
		s."CREDIT_AMOUNT", 
		s."DEBET_AMOUNT"
    FROM (SELECT DISTINCT * FROM stage.FT_POSTING_F) s
	ON CONFLICT DO NOTHING;

    -- Подсчитываем количество вставленных строк
    inserted_count := (SELECT COUNT(*) FROM DS.FT_POSTING_F);

	skipped_count := input_count - inserted_count - updated_count;

	-- Сообщение для лога
	SELECT STRING_AGG(
	    FORMAT(
	        'Несоответствие данных - oper_date: %s, credit_account_rk: %s, debet_account_rk: %s, credit_amount: %s, debet_amount: %s',
	        t."OPER_DATE", t."CREDIT_ACCOUNT_RK", t."DEBET_ACCOUNT_RK", t."CREDIT_AMOUNT", t."DEBET_AMOUNT"
	    ),
	    '; '
	)
	INTO message_for_log
	FROM (
	    SELECT 
	        CAST(s."OPER_DATE" AS DATE),
			CAST(s."CREDIT_ACCOUNT_RK" AS NUMERIC),
			CAST(s."DEBET_ACCOUNT_RK" AS NUMERIC),
			s."CREDIT_AMOUNT", 
			s."DEBET_AMOUNT"
	    FROM stage.FT_POSTING_F s
	    LEFT JOIN DS.ft_posting_f d ON 
			d.oper_date = CAST(s."OPER_DATE" AS DATE) 
			AND d.credit_account_rk = CAST(s."CREDIT_ACCOUNT_RK" AS NUMERIC)
			AND d.debet_account_rk = CAST(s."DEBET_ACCOUNT_RK" AS NUMERIC)
			AND d.credit_amount = s."CREDIT_AMOUNT"
			AND d.debet_amount = s."DEBET_AMOUNT"
	    WHERE
	        d.oper_date IS NULL AND d.credit_account_rk IS NULL AND d.debet_account_rk IS NULL
	) t;

    -- Возвращаем статистику
    RETURN QUERY SELECT input_count, inserted_count, updated_count, skipped_count, message_for_log;
END;
$$ LANGUAGE plpgsql;


-- 
CREATE OR REPLACE FUNCTION PROCESS_MD_CURRENCY_D(
) RETURNS TABLE(input_count INTEGER, inserted_count INTEGER, updated_count INTEGER, skipped_count INTEGER, message_for_log TEXT) AS $$
DECLARE
    input_count INTEGER := 0;
    inserted_count INTEGER := 0;
	before_insert_count INTEGER := 0;
    updated_count INTEGER := 0;
	skipped_count INTEGER := 0;
BEGIN
    -- Считаем полученные из csv строки в таблице stage
    input_count := (SELECT COUNT(*) FROM stage.MD_CURRENCY_D);

    -- Сначала обновляем строки, которые уже существуют в DS (с использованием JOIN)
    WITH updated_rows AS (
        UPDATE DS.MD_CURRENCY_D d
        SET 
			data_actual_end_date = CAST(s."DATA_ACTUAL_END_DATE" AS DATE),
			currency_code = CAST(s."CURRENCY_CODE" AS VARCHAR(3)),
			code_iso_char = CAST(s."CODE_ISO_CHAR" AS VARCHAR(3))
        FROM (SELECT DISTINCT * FROM stage.MD_CURRENCY_D) s
		WHERE
			d.currency_rk = CAST(s."CURRENCY_RK" AS NUMERIC) 
			AND d.data_actual_date = CAST(s."DATA_ACTUAL_DATE" AS DATE)
			AND (
				d.data_actual_end_date IS DISTINCT FROM CAST(s."DATA_ACTUAL_END_DATE" AS DATE) OR
				d.currency_code IS DISTINCT FROM CAST(s."CURRENCY_CODE" AS VARCHAR(3)) OR
				d.code_iso_char IS DISTINCT FROM CAST(s."CODE_ISO_CHAR" AS VARCHAR(3))
			)
			AND s."CURRENCY_CODE" IS NOT NULL 
    		AND s."CODE_ISO_CHAR" IS NOT NULL 
    		AND s."CURRENCY_CODE" BETWEEN 1 AND 998
    		AND s."CODE_ISO_CHAR" ~ '^[A-Z]{3}$' 
    		AND s."CODE_ISO_CHAR" NOT IN ('NON', '~')
        RETURNING d.currency_rk, d.data_actual_date  -- Для подсчета обновленных строк
    )
    -- Подсчитываем количество обновлений
    SELECT COUNT(*) INTO updated_count FROM updated_rows;

	before_insert_count := (SELECT COUNT(*) FROM DS.MD_CURRENCY_D);

    -- Затем вставляем в DS новые строки, которых нет в этой схеме
    INSERT INTO DS.MD_CURRENCY_D (currency_rk, data_actual_date, data_actual_end_date, currency_code, code_iso_char)
    SELECT 
		CAST(s."CURRENCY_RK" AS NUMERIC),
		CAST(s."DATA_ACTUAL_DATE" AS DATE),
		CAST(s."DATA_ACTUAL_END_DATE" AS DATE),
		CAST(s."CURRENCY_CODE" AS VARCHAR(3)),
		CAST(s."CODE_ISO_CHAR" AS VARCHAR(3))
    FROM (SELECT DISTINCT * FROM stage.MD_CURRENCY_D) s
    LEFT JOIN DS.MD_CURRENCY_D d ON d.currency_rk = CAST(s."CURRENCY_RK" AS NUMERIC) AND d.data_actual_date = CAST(s."DATA_ACTUAL_DATE" AS DATE)
    WHERE d.currency_rk IS NULL
		AND s."CURRENCY_CODE" IS NOT NULL 
    	AND s."CODE_ISO_CHAR" IS NOT NULL 
    	AND s."CURRENCY_CODE" BETWEEN 1 AND 998  -- TO_CHAR(s."CURRENCY_CODE", '999') ~ '^\d{3}$' 
    	AND s."CODE_ISO_CHAR" ~ '^[A-Z]{3}$' 
    	AND s."CODE_ISO_CHAR" NOT IN ('NON', '~')
	ON CONFLICT DO NOTHING;

    -- Подсчитываем количество вставленных строк
    inserted_count := (SELECT COUNT(*) FROM DS.MD_CURRENCY_D) - before_insert_count;

	skipped_count := input_count - inserted_count - updated_count;

	-- Сообщение для лога
	SELECT STRING_AGG(
	    FORMAT(
	        'Несоответствие данных - currency_rk: %s, data_actual_date: %s',
	        t."CURRENCY_RK", t."DATA_ACTUAL_DATE"
	    ),
	    '; '
	)
	INTO message_for_log
	FROM (
	    SELECT 
	        CAST(s."CURRENCY_RK" AS NUMERIC),
			CAST(s."DATA_ACTUAL_DATE" AS DATE)
	    FROM stage.MD_CURRENCY_D s
	    LEFT JOIN DS.MD_CURRENCY_D d ON d.currency_rk = CAST(s."CURRENCY_RK" AS NUMERIC) AND d.data_actual_date = CAST(s."DATA_ACTUAL_DATE" AS DATE)
	    WHERE
	        d.currency_rk IS NULL
	        OR (
	            d.data_actual_end_date IS DISTINCT FROM CAST(s."DATA_ACTUAL_END_DATE" AS DATE) OR
				d.currency_code IS DISTINCT FROM CAST(s."CURRENCY_CODE" AS VARCHAR(3)) OR
				d.code_iso_char IS DISTINCT FROM CAST(s."CODE_ISO_CHAR" AS VARCHAR(3))
	        )
	) t;

    -- Возвращаем статистику
    RETURN QUERY SELECT input_count, inserted_count, updated_count, skipped_count, message_for_log;
END;
$$ LANGUAGE plpgsql;


-- 
CREATE OR REPLACE FUNCTION PROCESS_MD_ACCOUNT_D(
) RETURNS TABLE(input_count INTEGER, inserted_count INTEGER, updated_count INTEGER, skipped_count INTEGER, message_for_log TEXT) AS $$
DECLARE
    input_count INTEGER := 0;
    inserted_count INTEGER := 0;
	before_insert_count INTEGER := 0;
    updated_count INTEGER := 0;
	skipped_count INTEGER := 0;
BEGIN
    -- Считаем полученные из csv строки в таблице stage
    input_count := (SELECT COUNT(*) FROM stage.MD_ACCOUNT_D);

    -- Сначала обновляем строки, которые уже существуют в DS (с использованием JOIN)
    WITH updated_rows AS (
        UPDATE DS.MD_ACCOUNT_D d
        SET 
			data_actual_end_date = CAST(s."DATA_ACTUAL_END_DATE" AS DATE),
			account_number = CAST(s."ACCOUNT_NUMBER" AS VARCHAR(20)),
			char_type = CAST(s."CHAR_TYPE" AS VARCHAR(1)),
			currency_rk = CAST(s."CURRENCY_RK" AS NUMERIC),
			currency_code = CAST(s."CURRENCY_CODE" AS VARCHAR(3))
        FROM (SELECT DISTINCT * FROM stage.MD_ACCOUNT_D) s
		WHERE
			d.data_actual_date = CAST(s."DATA_ACTUAL_DATE" AS DATE)
			AND d.account_rk = CAST(s."ACCOUNT_RK" AS NUMERIC)
			AND (
				d.data_actual_end_date IS DISTINCT FROM CAST(s."DATA_ACTUAL_END_DATE" AS DATE) OR
				d.account_number IS DISTINCT FROM CAST(s."ACCOUNT_NUMBER" AS VARCHAR(20)) OR
				d.char_type IS DISTINCT FROM CAST(s."CHAR_TYPE" AS VARCHAR(1)) OR
				d.currency_rk IS DISTINCT FROM CAST(s."CURRENCY_RK" AS NUMERIC) OR
				d.currency_code IS DISTINCT FROM CAST(s."CURRENCY_CODE" AS VARCHAR(3))
			)
        RETURNING d.data_actual_date, d.account_rk  -- Для подсчета обновленных строк
    )
    -- Подсчитываем количество обновлений
    SELECT COUNT(*) INTO updated_count FROM updated_rows;

	before_insert_count := (SELECT COUNT(*) FROM DS.MD_ACCOUNT_D);

    -- Затем вставляем в DS новые строки, которых нет в этой схеме
    INSERT INTO DS.MD_ACCOUNT_D (data_actual_date, data_actual_end_date, account_rk, account_number, char_type, currency_rk, currency_code)
    SELECT
	    CAST(s."DATA_ACTUAL_DATE" AS DATE),
		CAST(s."DATA_ACTUAL_END_DATE" AS DATE),
		CAST(s."ACCOUNT_RK" AS NUMERIC),
		CAST(s."ACCOUNT_NUMBER" AS VARCHAR(20)),
		CAST(s."CHAR_TYPE" AS VARCHAR(1)),
		CAST(s."CURRENCY_RK" AS NUMERIC),
		CAST(s."CURRENCY_CODE" AS VARCHAR(3))
    FROM (SELECT DISTINCT * FROM stage.MD_ACCOUNT_D) s
    LEFT JOIN DS.MD_ACCOUNT_D d ON d.data_actual_date = CAST(s."DATA_ACTUAL_DATE" AS DATE) AND d.account_rk = CAST(s."ACCOUNT_RK" AS NUMERIC)
    WHERE d.account_rk IS NULL
	ON CONFLICT DO NOTHING;

    -- Подсчитываем количество вставленных строк
    inserted_count := (SELECT COUNT(*) FROM DS.MD_ACCOUNT_D) - before_insert_count;

	skipped_count := input_count - inserted_count - updated_count;

	-- Сообщение для лога
	SELECT STRING_AGG(
	    FORMAT(
	        'Несоответствие данных - data_actual_date: %s, account_rk: %s',
	        t."DATA_ACTUAL_DATE", t."ACCOUNT_RK"
	    ),
	    '; '
	)
	INTO message_for_log
	FROM (
	    SELECT 
	        CAST(s."DATA_ACTUAL_DATE" AS DATE),
			CAST(s."ACCOUNT_RK" AS NUMERIC)
	    FROM stage.MD_ACCOUNT_D s
	    LEFT JOIN DS.MD_ACCOUNT_D d ON d.data_actual_date = CAST(s."DATA_ACTUAL_DATE" AS DATE) AND d.account_rk = CAST(s."ACCOUNT_RK" AS NUMERIC)
	    WHERE
	        d.data_actual_date IS NULL
	        OR (
	            d.data_actual_end_date IS DISTINCT FROM CAST(s."DATA_ACTUAL_END_DATE" AS DATE) OR
				d.account_number IS DISTINCT FROM CAST(s."ACCOUNT_NUMBER" AS VARCHAR(20)) OR
				d.char_type IS DISTINCT FROM CAST(s."CHAR_TYPE" AS VARCHAR(1)) OR
				d.currency_rk IS DISTINCT FROM CAST(s."CURRENCY_RK" AS NUMERIC) OR
				d.currency_code IS DISTINCT FROM CAST(s."CURRENCY_CODE" AS VARCHAR(3))
	        )
	) t;

    -- Возвращаем статистику
    RETURN QUERY SELECT input_count, inserted_count, updated_count, skipped_count, message_for_log;
END;
$$ LANGUAGE plpgsql;



-- 
CREATE OR REPLACE FUNCTION PROCESS_MD_EXCHANGE_RATE_D(
) RETURNS TABLE(input_count INTEGER, inserted_count INTEGER, updated_count INTEGER, skipped_count INTEGER, message_for_log TEXT) AS $$
DECLARE
    input_count INTEGER := 0;
    inserted_count INTEGER := 0;
	before_insert_count INTEGER := 0;
    updated_count INTEGER := 0;
	skipped_count INTEGER := 0;
BEGIN
    -- Считаем полученные из csv строки в таблице stage
    input_count := (SELECT COUNT(*) FROM stage.MD_EXCHANGE_RATE_D);

    -- Сначала обновляем строки, которые уже существуют в DS (с использованием JOIN)
    WITH updated_rows AS (
        UPDATE DS.MD_EXCHANGE_RATE_D d
        SET 
			data_actual_end_date = CAST(s."DATA_ACTUAL_END_DATE" AS DATE),
			reduced_cource = s."REDUCED_COURCE",
			code_iso_num = CAST(s."CODE_ISO_NUM" AS VARCHAR(3))
        FROM (SELECT DISTINCT * FROM stage.MD_EXCHANGE_RATE_D) s
		WHERE
			d.data_actual_date = CAST(s."DATA_ACTUAL_DATE" AS DATE) 
			AND d.currency_rk = CAST(s."CURRENCY_RK" AS NUMERIC)
			AND (
				d.data_actual_end_date IS DISTINCT FROM CAST(s."DATA_ACTUAL_END_DATE" AS DATE) OR
				d.reduced_cource IS DISTINCT FROM s."REDUCED_COURCE" OR
				d.code_iso_num IS DISTINCT FROM CAST(s."CODE_ISO_NUM" AS VARCHAR(3))
			)
        RETURNING d.data_actual_date, d.currency_rk  -- Для подсчета обновленных строк
    )
    -- Подсчитываем количество обновлений
    SELECT COUNT(*) INTO updated_count FROM updated_rows;

	before_insert_count := (SELECT COUNT(*) FROM DS.MD_EXCHANGE_RATE_D);

    -- Затем вставляем в DS новые строки, которых нет в этой схеме
    INSERT INTO DS.MD_EXCHANGE_RATE_D (data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num)
    SELECT
	    CAST(s."DATA_ACTUAL_DATE" AS DATE),
		CAST(s."DATA_ACTUAL_END_DATE" AS DATE),
		CAST(s."CURRENCY_RK" AS NUMERIC),
		s."REDUCED_COURCE",
		CAST(s."CODE_ISO_NUM" AS VARCHAR(3))
    FROM (SELECT DISTINCT * FROM stage.MD_EXCHANGE_RATE_D) s
    LEFT JOIN DS.MD_EXCHANGE_RATE_D d ON d.data_actual_date = CAST(s."DATA_ACTUAL_DATE" AS DATE) AND d.currency_rk = CAST(s."CURRENCY_RK" AS NUMERIC)
    WHERE d.currency_rk IS NULL
	ON CONFLICT DO NOTHING;

    -- Подсчитываем количество вставленных строк
    inserted_count := (SELECT COUNT(*) FROM DS.MD_EXCHANGE_RATE_D) - before_insert_count;

	skipped_count := input_count - inserted_count - updated_count;

	-- Сообщение для лога
	SELECT STRING_AGG(
	    FORMAT(
	        'Несоответствие данных - data_actual_date: %s, currency_rk: %s',
	        t."DATA_ACTUAL_DATE", t."CURRENCY_RK"
	    ),
	    '; '
	)
	INTO message_for_log
	FROM (
	    SELECT 
	        CAST(s."DATA_ACTUAL_DATE" AS DATE),
			CAST(s."CURRENCY_RK" AS NUMERIC)
	    FROM stage.MD_EXCHANGE_RATE_D s
	    LEFT JOIN DS.MD_EXCHANGE_RATE_D d ON d.data_actual_date = CAST(s."DATA_ACTUAL_DATE" AS DATE) AND d.currency_rk = CAST(s."CURRENCY_RK" AS NUMERIC)
	    WHERE
	        d.data_actual_date IS NULL
	        OR (
	            d.data_actual_end_date IS DISTINCT FROM CAST(s."DATA_ACTUAL_END_DATE" AS DATE) OR
				d.reduced_cource IS DISTINCT FROM s."REDUCED_COURCE" OR
				d.code_iso_num IS DISTINCT FROM CAST(s."CODE_ISO_NUM" AS VARCHAR(3))
	        )
	) t;

    -- Возвращаем статистику
    RETURN QUERY SELECT input_count, inserted_count, updated_count, skipped_count, message_for_log;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION PROCESS_MD_LEDGER_ACCOUNT_S(
) RETURNS TABLE(input_count INTEGER, inserted_count INTEGER, updated_count INTEGER, skipped_count INTEGER, message_for_log TEXT) AS $$
DECLARE
    input_count INTEGER := 0;
    inserted_count INTEGER := 0;
	before_insert_count INTEGER := 0;
    updated_count INTEGER := 0;
	skipped_count INTEGER := 0;
BEGIN
    -- Считаем полученные из csv строки в таблице stage
    input_count := (SELECT COUNT(*) FROM stage.MD_LEDGER_ACCOUNT_S);

    -- Сначала обновляем строки, которые уже существуют в DS (с использованием JOIN)
    WITH updated_rows AS (
        UPDATE DS.MD_LEDGER_ACCOUNT_S d
        SET 
			chapter = CAST(s."CHAPTER" AS CHAR(1)),
			chapter_name = CAST(s."CHAPTER_NAME" AS VARCHAR(16)),
			section_number = CAST(s."SECTION_NUMBER" AS INTEGER), 
			section_name = CAST(s."SECTION_NAME" AS VARCHAR(22)), 
			subsection_name = CAST(s."SUBSECTION_NAME" AS VARCHAR(21)),
			ledger1_account = CAST(s."LEDGER1_ACCOUNT" AS INTEGER),
			ledger1_account_name = CAST(s."LEDGER1_ACCOUNT_NAME" AS VARCHAR(47)),
			ledger_account_name = CAST(s."LEDGER_ACCOUNT_NAME" AS VARCHAR(153)),
			characteristic = CAST(s."CHARACTERISTIC" AS CHAR(1)),
			end_date = CAST(s."END_DATE" AS DATE)
        FROM (SELECT DISTINCT * FROM stage.MD_LEDGER_ACCOUNT_S) s
		WHERE
			d.ledger_account = CAST(s."LEDGER_ACCOUNT" AS INTEGER)
			AND d.start_date = CAST(s."START_DATE" AS DATE)
			AND (
				d.chapter IS DISTINCT FROM CAST(s."CHAPTER" AS CHAR(1)) OR
				d.chapter_name IS DISTINCT FROM CAST(s."CHAPTER_NAME" AS VARCHAR(16)) OR
				d.section_number IS DISTINCT FROM CAST(s."SECTION_NUMBER" AS INTEGER) OR
				d.section_name IS DISTINCT FROM CAST(s."SECTION_NAME" AS VARCHAR(22)) OR
				d.subsection_name IS DISTINCT FROM CAST(s."SUBSECTION_NAME" AS VARCHAR(21)) OR
				d.ledger1_account IS DISTINCT FROM CAST(s."LEDGER1_ACCOUNT" AS INTEGER) OR
				d.ledger1_account_name IS DISTINCT FROM CAST(s."LEDGER1_ACCOUNT_NAME" AS VARCHAR(47)) OR
				d.ledger_account_name IS DISTINCT FROM CAST(s."LEDGER_ACCOUNT_NAME" AS VARCHAR(153)) OR
				d.characteristic IS DISTINCT FROM CAST(s."CHARACTERISTIC" AS CHAR(1)) OR
				d.end_date IS DISTINCT FROM CAST(s."END_DATE" AS DATE)
			)
        RETURNING d.ledger_account, d.start_date  -- Для подсчета обн  вленных строк
    )
    -- Подсчитываем количество обновлений
    SELECT COUNT(*) INTO updated_count FROM updated_rows;

	before_insert_count := (SELECT COUNT(*) FROM DS.MD_LEDGER_ACCOUNT_S);

    -- Затем вставляем в DS новые строки, которых нет в этой схеме
    INSERT INTO DS.MD_LEDGER_ACCOUNT_S (chapter, chapter_name, section_number, section_name, subsection_name, ledger1_account,
	ledger1_account_name, ledger_account, ledger_account_name, characteristic, start_date, end_date)
    SELECT
	    CAST(s."CHAPTER" AS CHAR(1)),
		CAST(s."CHAPTER_NAME" AS VARCHAR(16)),
		CAST(s."SECTION_NUMBER" AS INTEGER), 
		CAST(s."SECTION_NAME" AS VARCHAR(22)), 
		CAST(s."SUBSECTION_NAME" AS VARCHAR(21)),
		CAST(s."LEDGER1_ACCOUNT" AS INTEGER),
		CAST(s."LEDGER1_ACCOUNT_NAME" AS VARCHAR(47)),
		CAST(s."LEDGER_ACCOUNT" AS INTEGER),
		CAST(s."LEDGER_ACCOUNT_NAME" AS VARCHAR(153)),
		CAST(s."CHARACTERISTIC" AS CHAR(1)),
		CAST(s."START_DATE" AS DATE),
		CAST(s."END_DATE" AS DATE)
    FROM (SELECT DISTINCT * FROM stage.MD_LEDGER_ACCOUNT_S) s
    LEFT JOIN DS.MD_LEDGER_ACCOUNT_S d ON d.ledger_account = CAST(s."LEDGER_ACCOUNT" AS INTEGER) AND d.start_date = CAST(s."START_DATE" AS DATE)
    WHERE d.ledger_account IS NULL
	ON CONFLICT DO NOTHING;

    -- Подсчитываем количество вставленных строк
    inserted_count := (SELECT COUNT(*) FROM DS.MD_LEDGER_ACCOUNT_S) - before_insert_count;

	skipped_count := input_count - inserted_count - updated_count;

	-- Сообщение для лога
	SELECT STRING_AGG(
	    FORMAT(
	        'Несоответствие данных - ledger_account: %s, start_date: %s',
	        t."LEDGER_ACCOUNT", t."START_DATE"
	    ),
	    '; '
	)
	INTO message_for_log
	FROM (
	    SELECT 
	        CAST(s."LEDGER_ACCOUNT" AS INTEGER),
			CAST(s."START_DATE" AS DATE)
	    FROM stage.MD_LEDGER_ACCOUNT_S s
	    LEFT JOIN DS.MD_LEDGER_ACCOUNT_S d ON d.ledger_account = CAST(s."LEDGER_ACCOUNT" AS INTEGER) AND d.start_date = CAST(s."START_DATE" AS DATE)
	    WHERE
	        d.ledger_account IS NULL
	        OR (
	            d.chapter IS DISTINCT FROM CAST(s."CHAPTER" AS CHAR(1)) OR
				d.chapter_name IS DISTINCT FROM CAST(s."CHAPTER_NAME" AS VARCHAR(16)) OR
				d.section_number IS DISTINCT FROM CAST(s."SECTION_NUMBER" AS INTEGER) OR
				d.section_name IS DISTINCT FROM CAST(s."SECTION_NAME" AS VARCHAR(22)) OR
				d.subsection_name IS DISTINCT FROM CAST(s."SUBSECTION_NAME" AS VARCHAR(21)) OR
				d.ledger1_account IS DISTINCT FROM CAST(s."LEDGER1_ACCOUNT" AS INTEGER) OR
				d.ledger1_account_name IS DISTINCT FROM CAST(s."LEDGER1_ACCOUNT_NAME" AS VARCHAR(47)) OR
				d.ledger_account_name IS DISTINCT FROM CAST(s."LEDGER_ACCOUNT_NAME" AS VARCHAR(153)) OR
				d.characteristic IS DISTINCT FROM CAST(s."CHARACTERISTIC" AS CHAR(1)) OR
				d.end_date IS DISTINCT FROM CAST(s."END_DATE" AS DATE)
	        )
	) t;

    -- Возвращаем статистику
    RETURN QUERY SELECT input_count, inserted_count, updated_count, skipped_count, message_for_log;
END;
$$ LANGUAGE plpgsql;
