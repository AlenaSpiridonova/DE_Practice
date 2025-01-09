CREATE USER dm_user WITH PASSWORD 'dm_password';
ALTER SCHEMA DM OWNER TO dm_user;


CREATE TABLE DM.DM_ACCOUNT_TURNOVER_F(
	on_date DATE,
	account_rk NUMERIC,
	credit_amount NUMERIC(23,8),
	credit_amount_rub NUMERIC(23,8),
	debet_amount NUMERIC(23,8),
	debet_amount_rub NUMERIC(23,8),
	PRIMARY KEY (on_date, account_rk) -- создала первичный ключ для улучшения взаимосвязи между витринами данных
);


CREATE TABLE DM.DM_ACCOUNT_BALANCE_F(
	on_date DATE, 
    account_rk NUMERIC,
    balance_out NUMERIC(23,8),
	balance_out_rub NUMERIC(23,8),
    PRIMARY KEY (on_date, account_rk) -- создала первичный ключ для улучшения взаимосвязи между витринами данных
);


-- Создание процедуры для расчета оборотов по лицевым счетам
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER := 0;
	inserted INTEGER := 0;
	curr_log_id INT;
BEGIN
	INSERT INTO LOGS.etl_logs (process, table_name, start_time, status, message)
	VALUES (
		'data_mart_load', 
		'DM.DM_ACCOUNT_TURNOVER_F', 
		NOW(), 
		'STARTED', 
		'Data load started'
	)
	RETURNING log_id INTO curr_log_id;
	
	DELETE FROM DM.DM_ACCOUNT_TURNOVER_F  -- чтобы можно было делать перерасчет за одну и ту же дату
	WHERE on_date = i_OnDate;
	
    INSERT INTO DM.DM_ACCOUNT_TURNOVER_F (
        on_date, 
        account_rk, 
        credit_amount, 
        credit_amount_rub, 
        debet_amount, 
        debet_amount_rub
    )
    SELECT 
        i_OnDate AS on_date,
        all_operations.account_rk,
        SUM(credit_amount) AS credit_amount,
        SUM(credit_amount * COALESCE(exrt.reduced_cource, 1)) AS credit_amount_rub,
        SUM(debet_amount) AS debet_amount,
        SUM(debet_amount * COALESCE(exrt.reduced_cource, 1)) AS debet_amount_rub
    FROM (
        -- Кредитовые проводки
        SELECT 
            fpf.credit_account_rk AS account_rk,
            fpf.credit_amount AS credit_amount,
            0 AS debet_amount,
            fpf.oper_date
        FROM DS.FT_POSTING_F fpf
        WHERE fpf.oper_date = i_OnDate
        
        UNION ALL
        
        -- Дебетовые проводки
        SELECT 
            fpf.debet_account_rk AS account_rk,
            0 AS credit_amount,
            fpf.debet_amount AS debet_amount,
            fpf.oper_date
        FROM DS.FT_POSTING_F fpf
        WHERE fpf.oper_date = i_OnDate
    ) AS all_operations
	LEFT JOIN DS.MD_ACCOUNT_D accd ON all_operations.account_rk = accd.account_rk
    LEFT JOIN DS.MD_EXCHANGE_RATE_D exrt 
		ON accd.currency_rk = exrt.currency_rk
		AND all_operations.oper_date BETWEEN exrt.data_actual_date AND exrt.data_actual_end_date
    GROUP BY all_operations.account_rk, exrt.reduced_cource;

	GET DIAGNOSTICS inserted = ROW_COUNT;

	UPDATE LOGS.etl_logs
        SET 
			status='SUCCESS', 
			end_time=NOW(), 
			message='Добавлены данные за ' || i_OnDate::TEXT, 
			input_rows=inserted, 
			inserted_rows=inserted, 
			updated_rows=0, 
			skipped_rows=0
        WHERE log_id=curr_log_id;
    
    RAISE NOTICE 'Процедура ds.fill_account_turnover_f: расчет оборотов по лицевым счетам за % выполнен успешно', i_OnDate;

EXCEPTION
    WHEN OTHERS THEN
	UPDATE LOGS.etl_logs
    	SET 
        	status = 'FAILURE', 
        	end_time = NOW(), 
        	message = 'Ошибка при расчете оборотов за ' || i_OnDate::TEXT || ': ' || SQLERRM, 
        	input_rows = 0, 
        	inserted_rows = 0, 
        	updated_rows = 0, 
        	skipped_rows = 0
        WHERE log_id = curr_log_id;
		
        RAISE WARNING 'Процедура ds.fill_account_turnover_f: расчет оборотов по лицевым счетам за % не выполнен: %', i_OnDate, SQLERRM;
END;
$$;



-- Создание процедуры для расчета оборотов за конкретный период или число (если нужно за какое-то одно число, оно указывается как начало и 
-- конец периода)
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f_by_period(start_date DATE, end_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
	calc_date DATE := start_date;
BEGIN
    WHILE calc_date <= end_date LOOP
        CALL ds.fill_account_turnover_f(calc_date);
        calc_date := calc_date + INTERVAL '1 day';
    END LOOP;
END;
$$;


-- Вставка данных за 31.12.2017 г. в витрину данных DM.DM_ACCOUNT_BALANCE_F
INSERT INTO DM.DM_ACCOUNT_BALANCE_F (on_date, account_rk, balance_out, balance_out_rub)
SELECT 
    '2017-12-31'::DATE AS on_date,
    account_rk,
    balance_out,
    balance_out * COALESCE(exrt.reduced_cource, 1) AS balance_out_rub
FROM 
	DS.FT_BALANCE_F fbf
LEFT JOIN DS.MD_EXCHANGE_RATE_D exrt 
    ON fbf.currency_rk = exrt.currency_rk
	AND '2017-12-31'::DATE BETWEEN exrt.data_actual_date AND exrt.data_actual_end_date;
	


-- Создание процедуры для расчета остатков по лицевым счетам
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER := 0;
	inserted INTEGER := 0;
	curr_log_id INT;
BEGIN
	INSERT INTO LOGS.etl_logs (process, table_name, start_time, status, message)
	VALUES (
		'data_mart_load', 
		'DM.DM_ACCOUNT_BALANCE_F', 
		NOW(), 
		'STARTED', 
		'Data load started'
	)
	RETURNING log_id INTO curr_log_id;
			
	DELETE FROM DM.DM_ACCOUNT_BALANCE_F  -- чтобы можно было делать перерасчет за одну и ту же дату
	WHERE on_date = i_OnDate;
	
    INSERT INTO DM.DM_ACCOUNT_BALANCE_F (on_date, account_rk, balance_out, balance_out_rub)
    SELECT 
        i_OnDate AS on_date,
        accd.account_rk,
        CASE 
            WHEN accd.char_type = 'А' THEN COALESCE(dmb.balance_out, 0) + COALESCE(dmt.debet_amount, 0) - COALESCE(dmt.credit_amount, 0)
            WHEN accd.char_type = 'П' THEN COALESCE(dmb.balance_out, 0) - COALESCE(dmt.debet_amount, 0) + COALESCE(dmt.credit_amount, 0)
        END AS balance_out,
        CASE 
            WHEN accd.char_type = 'А' THEN COALESCE(dmb.balance_out_rub, 0) + COALESCE(dmt.debet_amount_rub, 0) - COALESCE(dmt.credit_amount_rub, 0)
            WHEN accd.char_type = 'П' THEN COALESCE(dmb.balance_out_rub, 0) - COALESCE(dmt.debet_amount_rub, 0) + COALESCE(dmt.credit_amount_rub, 0)
        END AS balance_out_rub
    FROM DS.MD_ACCOUNT_D accd
    LEFT JOIN DM.DM_ACCOUNT_BALANCE_F dmb
        ON dmb.account_rk = accd.account_rk AND dmb.on_date = i_OnDate - INTERVAL '1 day'
    LEFT JOIN DM.DM_ACCOUNT_TURNOVER_F dmt 
        ON dmt.account_rk = accd.account_rk AND dmt.on_date = i_OnDate
    WHERE i_OnDate BETWEEN accd.data_actual_date AND COALESCE(accd.data_actual_end_date, i_OnDate); -- COALESCE - на случай значений null

	GET DIAGNOSTICS inserted = ROW_COUNT;

	UPDATE LOGS.etl_logs
        SET 
			status='SUCCESS', 
			end_time=NOW(), 
			message='Добавлены данные за ' || i_OnDate::TEXT, 
			input_rows=inserted, 
			inserted_rows=inserted, 
			updated_rows=0, 
			skipped_rows=0
        WHERE log_id=curr_log_id;
    
    RAISE NOTICE 'Процедура ds.fill_account_balance_f: расчет остатков по лицевым счетам за % выполнен успешно', i_OnDate;
EXCEPTION
    WHEN OTHERS THEN
		UPDATE LOGS.etl_logs
    	SET 
        	status = 'FAILURE', 
        	end_time = NOW(), 
        	message = 'Ошибка при расчете остатков за ' || i_OnDate::TEXT || ': ' || SQLERRM, 
        	input_rows = 0, 
        	inserted_rows = 0, 
        	updated_rows = 0, 
        	skipped_rows = 0
        WHERE log_id = curr_log_id;
		
        RAISE WARNING 'Процедура ds.fill_account_turnover_f: расчет остатков по лицевым счетам за % не выполнен: %', i_OnDate, SQLERRM;
END;
$$;



-- Создание процедуры для расчета остатков за конкретный период или число (если нужно за какое-то одно число, оно указывается как начало и
-- конец периода)
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f_by_period(start_date DATE, end_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
	calc_date DATE := start_date;
BEGIN
    WHILE calc_date <= end_date LOOP
        CALL ds.fill_account_balance_f(calc_date);
        calc_date := calc_date + INTERVAL '1 day';
    END LOOP;
END;
$$;



-- Код для проверки

CALL ds.fill_account_turnover_f_by_period('2018-01-01', '2018-01-31');
CALL ds.fill_account_balance_f_by_period('2018-01-01', '2018-01-31');


SELECT * FROM DM.DM_ACCOUNT_BALANCE_F
WHERE account_rk = 34157147;

SELECT * FROM DM.DM_ACCOUNT_TURNOVER_F
WHERE account_rk = 34157147;

SELECT * FROM logs.etl_logs;

SELECT * FROM DM.DM_ACCOUNT_TURNOVER_F
WHERE on_date = '2018-01-10';

SELECT * FROM DM.DM_ACCOUNT_BALANCE_F
WHERE on_date = '2018-01-10';


TRUNCATE TABLE DM.DM_ACCOUNT_BALANCE_F RESTART IDENTITY;
TRUNCATE TABLE DM.DM_ACCOUNT_TURNOVER_F RESTART IDENTITY;
TRUNCATE TABLE LOGS.etl_logs RESTART IDENTITY;