-- Задание 2.3

-- Предварительная проверка данных в таблице-источнике на непрерывность следования дат и возможный пропуск дней
WITH ordered_data AS (
    SELECT 
        account_rk,
        effective_date,
        LEAD(effective_date) OVER (PARTITION BY account_rk ORDER BY effective_date) AS next_date
    FROM rd.account_balance
)
SELECT 
	account_rk,
	effective_date,
	next_date,
	next_date - effective_date AS date_diff
FROM ordered_data
WHERE next_date - INTERVAL '1 day' <> effective_date; 
-- Результат: 0 строк. Даты идут непрерывно, без пропуска дней, поэтому для копирования остатков можно использовать строго 
-- предыдущий/следующий день (effective_date - INTERVAL '1 day'), а не оконную функцию, которая могла бы упорядочить любые даты 
-- (в т.ч. с пропусками дней) для переноса остатков между ними. Кроме того, потенциально перенос остатков через пропущенные даты может 
-- являться некорректным поведением.


-- Пункт 1: "Подготовить запрос, который определит корректное значение поля account_in_sum. Если значения полей account_in_sum одного дня и 
-- account_out_sum предыдущего дня отличаются, то корректным выбирается значение account_out_sum предыдущего дня."

SELECT
	ab2.account_rk,
	ab2.effective_date,
	ab1.account_out_sum AS correct_account_in_sum
FROM 
	rd.account_balance ab1
INNER JOIN rd.account_balance ab2 
	ON ab1.account_rk = ab2.account_rk
	AND ab1.effective_date = ab2.effective_date - INTERVAL '1 day'
WHERE 
	ab2.account_in_sum <> ab1.account_out_sum;



-- Пункт 2: "Подготовить такой же запрос, только проблема теперь в том, что account_in_sum одного дня правильная, а account_out_sum 
-- предыдущего дня некорректна. Это означает, что если эти значения отличаются, то корректным значением для account_out_sum предыдущего 
-- дня выбирается значение account_in_sum текущего дня."
SELECT 
	ab1.account_rk,
	ab1.effective_date,
	ab2.account_in_sum AS correct_account_out_sum
FROM 
	rd.account_balance ab1
INNER JOIN rd.account_balance ab2 
	ON ab1.account_rk = ab2.account_rk
	AND ab1.effective_date = ab2.effective_date - INTERVAL '1 day'
WHERE 
	ab2.account_in_sum <> ab1.account_out_sum;


-- Пункт 3: "подготовить запрос, который поправит данные в таблице rd.account_balance используя уже имеющийся запрос из п.1"
WITH previous_day AS (
    SELECT
        ab2.account_rk,
        ab2.effective_date,
        ab1.account_out_sum AS correct_account_in_sum
    FROM 
		rd.account_balance ab1
    INNER JOIN rd.account_balance ab2 
        ON ab1.account_rk = ab2.account_rk
        AND ab1.effective_date = ab2.effective_date - INTERVAL '1 day'
    WHERE 
		ab2.account_in_sum <> ab1.account_out_sum
)
UPDATE rd.account_balance ab
SET account_in_sum = pd.correct_account_in_sum
FROM 
	previous_day pd
WHERE 
	ab.account_rk = pd.account_rk
	AND ab.effective_date = pd.effective_date;


-- Процедура перезагрузки данных в витрину dm.account_balance_turnover
CREATE OR REPLACE PROCEDURE reload_account_balance_turnover()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Очистка витрины
    TRUNCATE TABLE dm.account_balance_turnover;

    -- Перезагрузка данных
    INSERT INTO dm.account_balance_turnover (
        account_rk,
        currency_name,
        department_rk,
        effective_date,
        account_in_sum,
        account_out_sum
    )
    SELECT 
        a.account_rk,
        COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
        a.department_rk,
        ab.effective_date,
        ab.account_in_sum,
        ab.account_out_sum
    FROM 
		rd.account a
    LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
    LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;

    RAISE NOTICE 'Данные в витрине dm.account_balance_turnover успешно перезагружены.';
END;
$$;

CALL reload_account_balance_turnover();
