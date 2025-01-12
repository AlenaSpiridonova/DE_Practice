-- Создание таблицы DM.DM_F101_ROUND_F
CREATE TABLE DM.DM_F101_ROUND_F (
FROM_DATE DATE,
TO_DATE DATE,
CHAPTER CHAR(1),
LEDGER_ACCOUNT CHAR(5),
CHARACTERISTIC CHAR(1),
BALANCE_IN_RUB NUMERIC(23,8),
R_BALANCE_IN_RUB NUMERIC(23,8),
BALANCE_IN_VAL NUMERIC(23,8),
R_BALANCE_IN_VAL NUMERIC(23,8),
BALANCE_IN_TOTAL NUMERIC(23,8),
R_BALANCE_IN_TOTAL NUMERIC(23,8),
TURN_DEB_RUB NUMERIC(23,8),
R_TURN_DEB_RUB NUMERIC(23,8),
TURN_DEB_VAL NUMERIC(23,8),
R_TURN_DEB_VAL NUMERIC(23,8),
TURN_DEB_TOTAL NUMERIC(23,8),
R_TURN_DEB_TOTAL NUMERIC(23,8),
TURN_CRE_RUB NUMERIC(23,8),
R_TURN_CRE_RUB NUMERIC(23,8), -- удалила лишнюю скобку
TURN_CRE_VAL NUMERIC(23,8),
R_TURN_CRE_VAL NUMERIC(23,8),
TURN_CRE_TOTAL NUMERIC(23,8),
R_TURN_CRE_TOTAL NUMERIC(23,8),
BALANCE_OUT_RUB NUMERIC(23,8),
R_BALANCE_OUT_RUB NUMERIC(23,8),
BALANCE_OUT_VAL NUMERIC(23,8),
R_BALANCE_OUT_VAL NUMERIC(23,8),
BALANCE_OUT_TOTAL NUMERIC(23,8),
R_BALANCE_OUT_TOTAL NUMERIC(23,8),
PRIMARY KEY (FROM_DATE, TO_DATE, CHAPTER, LEDGER_ACCOUNT, CHARACTERISTIC) -- добавила первичный ключ для обеспечения целостности данных
);


-- Процедура по созданию витрины для расчета 101 формы
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    init_date DATE := i_OnDate - INTERVAL '1 month'; -- Первый день отчетного периода
    final_date DATE := i_OnDate - INTERVAL '1 day';    -- Последний день отчетного периода
	inserted_count INTEGER := 0;
	inserted INTEGER := 0;
	curr_log_id INT;
BEGIN
	-- Логирование начала процесса
	INSERT INTO LOGS.etl_logs (process, table_name, start_time, status, message)
	VALUES (
		'data_mart_load', 
		'DM.DM_F101_ROUND_F', 
		NOW(), 
		'STARTED', 
		'Data load started'
	)
	RETURNING log_id INTO curr_log_id;

    -- Удаление старых записей за заданную дату
    DELETE FROM DM.DM_F101_ROUND_F
    WHERE TO_DATE = final_date;

    -- Вставка новых данных
    INSERT INTO DM.DM_F101_ROUND_F (
        FROM_DATE, TO_DATE, CHAPTER, LEDGER_ACCOUNT, CHARACTERISTIC, 
        BALANCE_IN_RUB, BALANCE_IN_VAL, BALANCE_IN_TOTAL, 
        TURN_DEB_RUB, TURN_DEB_VAL, TURN_DEB_TOTAL, 
        TURN_CRE_RUB, TURN_CRE_VAL, TURN_CRE_TOTAL, 
        BALANCE_OUT_RUB, BALANCE_OUT_VAL, BALANCE_OUT_TOTAL
    )
    SELECT
        init_date AS FROM_DATE,
        final_date AS TO_DATE,
        las.chapter AS CHAPTER,
        SUBSTRING(ad.account_number FROM 1 FOR 5) AS LEDGER_ACCOUNT,
        ad.char_type AS CHARACTERISTIC,
        -- Остатки на начало периода
        SUM(CASE WHEN ad.currency_code IN ('810', '643') THEN abf.balance_out_rub ELSE 0 END) AS BALANCE_IN_RUB,
        SUM(CASE WHEN ad.currency_code NOT IN ('810', '643') THEN abf.balance_out_rub ELSE 0 END) AS BALANCE_IN_VAL,
        SUM(abf.balance_out_rub) AS BALANCE_IN_TOTAL,
        -- Дебетовые обороты за период
        SUM(CASE WHEN ad.currency_code IN ('810', '643') THEN atf.debet_amount_rub ELSE 0 END) AS TURN_DEB_RUB,
        SUM(CASE WHEN ad.currency_code NOT IN ('810', '643') THEN atf.debet_amount_rub ELSE 0 END) AS TURN_DEB_VAL,
        SUM(atf.debet_amount_rub) AS TURN_DEB_TOTAL,
        -- Кредитовые обороты за период
        SUM(CASE WHEN ad.currency_code IN ('810', '643') THEN atf.credit_amount_rub ELSE 0 END) AS TURN_CRE_RUB,
        SUM(CASE WHEN ad.currency_code NOT IN ('810', '643') THEN atf.credit_amount_rub ELSE 0 END) AS TURN_CRE_VAL,
        SUM(atf.credit_amount_rub) AS TURN_CRE_TOTAL,
        -- Остатки на конец периода
        SUM(CASE WHEN ad.currency_code IN ('810', '643') THEN abf2.balance_out_rub ELSE 0 END) AS BALANCE_OUT_RUB,
        SUM(CASE WHEN ad.currency_code NOT IN ('810', '643') THEN abf2.balance_out_rub ELSE 0 END) AS BALANCE_OUT_VAL,
        SUM(abf2.balance_out_rub) AS BALANCE_OUT_TOTAL
    FROM
        DS.MD_ACCOUNT_D ad
    LEFT JOIN DS.MD_LEDGER_ACCOUNT_S las
        ON SUBSTRING(ad.account_number FROM 1 FOR 5)::INTEGER = las.ledger_account
    LEFT JOIN DM.DM_ACCOUNT_BALANCE_F abf
        ON ad.account_rk = abf.account_rk AND abf.on_date = init_date - INTERVAL '1 day'
	LEFT JOIN (
		SELECT -- запрос во избежание дублирования остатков на начало и конец периода
			datf.account_rk,
			SUM(datf.debet_amount_rub) AS debet_amount_rub,
			SUM(datf.credit_amount_rub) AS credit_amount_rub
		FROM 
			DM.DM_ACCOUNT_TURNOVER_F datf
        WHERE 
			datf.on_date BETWEEN init_date AND final_date
		GROUP BY 
			datf.account_rk
	) atf ON ad.account_rk = atf.account_rk
    LEFT JOIN DM.DM_ACCOUNT_BALANCE_F abf2
        ON ad.account_rk = abf2.account_rk AND abf2.on_date = final_date
    WHERE 
		ad.data_actual_date <= final_date
        AND (ad.data_actual_end_date IS NULL OR ad.data_actual_end_date >= init_date)
    GROUP BY
        las.chapter, SUBSTRING(ad.account_number FROM 1 FOR 5), ad.char_type;

	GET DIAGNOSTICS inserted = ROW_COUNT;
	-- Логирование завершения процесса
	UPDATE LOGS.etl_logs
        SET 
			status='SUCCESS', 
			end_time=NOW(), 
			message='Добавлены данные за отчетную дату ' || i_OnDate::TEXT, 
			input_rows=inserted, 
			inserted_rows=inserted, 
			updated_rows=0, 
			skipped_rows=0
        WHERE log_id=curr_log_id;

    RAISE NOTICE 'Процедура dm.fill_f101_round_f: расчет 101 формы за отчетную дату % завершен успешно', i_OnDate;

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
		
        RAISE WARNING 'Процедура dm.fill_f101_round_f: расчет 101 формы за отчетную дату % не выполнен: %', i_OnDate, SQLERRM;

END;
$$;


-- Код для проверки
CALL dm.fill_f101_round_f('2018-02-01');

SELECT * FROM DM.DM_F101_ROUND_F;

SELECT * FROM LOGS.etl_logs;


