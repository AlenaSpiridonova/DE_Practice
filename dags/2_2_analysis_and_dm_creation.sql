-- ЭТАП 1. Создание таблиц для импорта данных из csv-файлов
CREATE TABLE rd.deal_info_extra (LIKE rd.deal_info INCLUDING ALL);
CREATE TABLE rd.product_extra (LIKE rd.product INCLUDING ALL);


-- ЭТАП 2. Анализ данных в витрине dm.loan_holiday_info

-- Количество записей в витрине dm.loan_holiday_info: 10002
SELECT * FROM dm.loan_holiday_info;

-- Анализ витрины dm.loan_holiday_info по датам (столбцам effective_from_date и effective_to_date): 
-- 3 даты в столбце effective_from_date - 2023-01-01, 2023-03-15 и 2023-08-11, данные в столбце effective_to_date совпадают 
-- (одна дата - 2999-12-31). Отсюда следует, что данные из столбца effective_to_date можно не учитывать, и что значение имеет лишь 
-- столбец effective_from_date. 
-- Данные распределены почти равномерно: 3000, 3500 и 3502 записи соответственно.
SELECT 
	DISTINCT effective_from_date, 
	effective_to_date, 
	COUNT(*) 
FROM 
	dm.loan_holiday_info
GROUP BY 
	effective_from_date, 
	effective_to_date;
	

-- ЭТАП 3. Анализ данных в таблицах-источниках и csv-файлах

SELECT * FROM rd.loan_holiday; -- 10000 записей в источнике rd.loan_holiday


-- 3.1. Сравнение таблиц rd.deal_info и rd.deal_info_extra

SELECT * FROM rd.deal_info; -- 6500 записей в источнике rd.deal_info
SELECT * FROM rd.deal_info_extra; -- 3500 записей в deal_info.csv, загруженном в таблицу под названием rd.deal_info_extra
-- Вывод: в таблице-источнике почти в 2 раза больше записей, чем в csv-файле. Таким образом, данные в csv-файле являются неполными, 
-- и на них нельзя полагаться на 100 %.

-- Поиск различающихся строк в таблицах 
-- Записи из источника rd.deal_info, которых нет в csv (rd.deal_info_extra): 6500 записей.
SELECT *
FROM rd.deal_info
EXCEPT
SELECT *
FROM rd.deal_info_extra

-- Записи из csv (rd.deal_info_extra), которых нет в источнике rd.deal_info: 3500 записей.
SELECT *
FROM rd.deal_info_extra
EXCEPT
SELECT *
FROM rd.deal_info;

-- Количество совпадающих строк в rd.deal_info и rd.deal_info_extra: 0
SELECT COUNT(*)
FROM (
    SELECT *
    FROM rd.deal_info
    INTERSECT
    SELECT *
    FROM rd.deal_info_extra
) AS common_rows;
-- Вывод: можно дозагрузить новые данные, но удалять существующие нельзя, поскольку в таком случае они будут утрачены.


-- Анализ таблиц rd.deal_info и rd.deal_info_extra по датам (столбцам effective_from_date и effective_to_date)
-- Таблица rd.deal_info: 2 даты в столбце effective_from_date - 2023-01-01 и 2023-08-11, данные в столбце effective_to_date совпадают 
-- (одна дата - 2999-12-31). Не хватает данных по дате 2023-03-15.
SELECT 
	DISTINCT effective_from_date, 
	effective_to_date, 
	COUNT(*) 
FROM 
	rd.deal_info
GROUP BY 
	effective_from_date, 
	effective_to_date;


-- Таблица rd.deal_info_extra: 1 дата в столбце effective_from_date - 2023-03-15 - и одна дата в столбце effective_to_date - 2999-12-31.
-- Не хватает данных по датам 2023-01-01 и 2023-08-11.
SELECT 
	DISTINCT effective_from_date, 
	effective_to_date, 
	COUNT(*) 
FROM 
	rd.deal_info_extra
GROUP BY 
	effective_from_date, 
	effective_to_date;


-- Дубликаты строк (неполные) в таблице-источнике rd.deal_info: 2 строки
WITH doubles AS(
SELECT DISTINCT deal_rk, effective_from_date, COUNT(*) FROM rd.deal_info
GROUP BY deal_rk, effective_from_date
HAVING COUNT(*) > 1)
SELECT 
 di.*
FROM rd.deal_info di
INNER JOIN doubles d USING(deal_rk, effective_from_date)
ORDER BY deal_rk, effective_from_date;

-- Дубликаты строк (неполные) в csv-файле rd.deal_info_extra: 2 строки
WITH doubles AS(
SELECT DISTINCT deal_rk, effective_from_date FROM rd.deal_info_extra
GROUP BY deal_rk, effective_from_date
HAVING COUNT(*) > 1)
SELECT 
 die.*
FROM rd.deal_info_extra die
INNER JOIN doubles d USING(deal_rk, effective_from_date)
ORDER BY deal_rk, effective_from_date;
	

-- Дозагрузка данных из csv-файла rd.deal_info_extra
DO $$
BEGIN
    -- Вставка данных из csv-файла
    INSERT INTO rd.deal_info (deal_rk, deal_num, deal_name, deal_sum, client_rk, account_rk, agreement_rk, deal_start_date, department_rk, product_rk, deal_type_cd, effective_from_date, effective_to_date)
	SELECT DISTINCT deal_rk, deal_num, deal_name, deal_sum, client_rk, account_rk, agreement_rk, deal_start_date, department_rk, product_rk, deal_type_cd, effective_from_date, effective_to_date
	FROM rd.deal_info_extra;

    RAISE NOTICE 'Данные успешно перенесены из rd.deal_info_extra в rd.deal_info.';
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Ошибка: %', SQLERRM;
END $$;


SELECT * FROM rd.deal_info; 
-- Результат: после вставки данных в таблице-источнике rd.deal_info стало 10000 строк

-- Дубликаты строк (неполные) в таблице-источнике rd.deal_info после вставки данных: 4 строки
WITH doubles AS(
SELECT DISTINCT deal_rk, effective_from_date FROM rd.deal_info
GROUP BY deal_rk, effective_from_date
HAVING COUNT(*) > 1)
SELECT 
 di.*
FROM rd.deal_info di
INNER JOIN doubles d USING(deal_rk, effective_from_date)
ORDER BY deal_rk, effective_from_date;




-- 3.2. Сравнение таблиц rd.product и rd.product_extra

SELECT * FROM rd.product; -- 3500 записей в источнике rd.product
SELECT * FROM rd.product_extra; -- 10000 записей в product_info.csv, загруженном в таблицу под названием rd.product_extra
-- Вывод: в таблице-источнике почти в 3 раза меньше записей, чем в csv-файле.

-- Поиск различающихся строк в таблицах rd.product и rd.product_extra
-- Записи из источника rd.product, которых нет в csv (rd.product_extra): 12 записей. Все записи содержат опечатки в столбце product_name
-- в виде пропущенных букв в словах.
SELECT *
FROM rd.product
EXCEPT
SELECT *
FROM rd.product_extra;

-- Записи из csv (rd.product_extra), которых нет в источнике rd.product: 6510 новых записей.
SELECT *
FROM rd.product_extra
EXCEPT
SELECT *
FROM rd.product;

-- Количество совпадающих строк в rd.product и rd.product_extra: 3486
SELECT COUNT(*)
FROM (
    SELECT *
    FROM rd.product
    INTERSECT
    SELECT *
    FROM rd.product_extra
) AS common_rows;

-- 12 записей из источника rd.product, которых вроде бы нет в csv (rd.product_extra), на самом деле имеются в csv (rd.product_extra) 
-- в корректном виде, т. е. это те же самые данные, но без опечаток.
SELECT * FROM rd.product_extra
WHERE product_rk IN (SELECT ppe.product_rk FROM (
	SELECT *
	FROM rd.product
	EXCEPT
	SELECT *
	FROM rd.product_extra
	) ppe
);
-- Вывод: данные из источника rd.product можно перезагружать полностью, поскольку все они имеются в csv (rd.product_extra), к тому же 
-- в корректном виде. Кроме того, в csv (rd.product_extra) имеется 6510 новых записей.

-- Анализ таблиц rd.product и rd.product_extra по датам (столбцам effective_from_date и effective_to_date)
-- Таблица rd.product: 1 дата в столбце effective_from_date - 2023-03-15 - и 1 дата в столбце effective_to_date - 2999-12-31. Не хватает 
-- данных по датам 2023-01-01 и 2023-08-11.
SELECT 
	DISTINCT effective_from_date, 
	effective_to_date, 
	COUNT(*) 
FROM 
	rd.product
GROUP BY 
	effective_from_date, 
	effective_to_date;


-- Таблица rd.product_extra: есть все 3 даты в столбце effective_from_date - 2023-01-01, 2023-03-15 и 2023-08-11 - и одна дата в столбце 
-- effective_to_date - 2999-12-31.
SELECT 
	DISTINCT effective_from_date, 
	effective_to_date, 
	COUNT(*) 
FROM 
	rd.product_extra
GROUP BY 
	effective_from_date, 
	effective_to_date;

-- Дубликаты строк (полные и неполные) в таблице-источнике rd.product: 8 строк
WITH doubles AS(
SELECT DISTINCT product_rk, effective_from_date FROM rd.product
GROUP BY product_rk, effective_from_date
HAVING COUNT(*) > 1)
SELECT 
 p.*
FROM rd.product p
INNER JOIN doubles d USING(product_rk, effective_from_date)
ORDER BY product_rk, effective_from_date;

-- Дубликаты строк (полные и неполные) в csv-файле rd.product_extra: 36 строк
WITH doubles AS(
SELECT DISTINCT product_rk, effective_from_date FROM rd.product_extra
GROUP BY product_rk, effective_from_date
HAVING COUNT(*) > 1)
SELECT 
 pe.*
FROM rd.product_extra pe
INNER JOIN doubles d USING(product_rk, effective_from_date)
ORDER BY product_rk, effective_from_date;

-- Очистка таблицы-источника rd.product и загрузка данных из csv-файла rd.product_extra
DO $$
BEGIN
    -- Очистка таблицы-источника
    TRUNCATE TABLE rd.product RESTART IDENTITY;

    -- Вставка данных из csv-файла
    INSERT INTO rd.product (product_rk, product_name, effective_from_date, effective_to_date)
	SELECT DISTINCT product_rk, product_name, effective_from_date, effective_to_date
	FROM rd.product_extra;


    RAISE NOTICE 'Данные успешно перенесены из rd.product_extra в rd.product.';
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Ошибка: %', SQLERRM;
END $$;

SELECT * FROM rd.product; 
-- Результат: в таблицу-источник rd.product вставлено 9996 строк, удалено 4 строки, являющихся полными дубликатами

-- Дубликаты строк (неполные) в таблице-источнике rd.product после вставки данных: 28 строк
WITH doubles AS(
SELECT DISTINCT product_rk, effective_from_date FROM rd.product
GROUP BY product_rk, effective_from_date
HAVING COUNT(*) > 1)
SELECT 
 p.*
FROM rd.product p
INNER JOIN doubles d USING(product_rk, effective_from_date)
ORDER BY product_rk, effective_from_date;


-- Процедура перезагрузки данных в витрину ("ON 1=1" не стала удалять)
CREATE OR REPLACE PROCEDURE reload_dm_loan_holiday_info()
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1. Очистка витрины
    TRUNCATE TABLE dm.loan_holiday_info;

    -- 2. Перезагрузка данных в витрину
    WITH deal AS (
		SELECT  deal_rk
			   ,deal_num --Номер сделки
			   ,deal_name --Наименование сделки
			   ,deal_sum --Сумма сделки
			   ,client_rk --Ссылка на клиента
			   ,agreement_rk --Ссылка на договор
			   ,deal_start_date --Дата начала действия сделки
			   ,department_rk --Ссылка на отделение
			   ,product_rk -- Ссылка на продукт
			   ,deal_type_cd
			   ,effective_from_date
			   ,effective_to_date
		FROM RD.deal_info
		), loan_holiday AS (
		SELECT  deal_rk
			   ,loan_holiday_type_cd  --Ссылка на тип кредитных каникул
			   ,loan_holiday_start_date     --Дата начала кредитных каникул
			   ,loan_holiday_finish_date    --Дата окончания кредитных каникул
			   ,loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
			   ,loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
			   ,loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
			   ,effective_from_date
			   ,effective_to_date
		FROM RD.loan_holiday
		), product AS (
		SELECT  product_rk
			  ,product_name
			  ,effective_from_date
			  ,effective_to_date
		FROM RD.product
		), holiday_info AS (
		SELECT  d.deal_rk
		        ,lh.effective_from_date
		        ,lh.effective_to_date
		        ,d.deal_num AS deal_number --Номер сделки
			    ,lh.loan_holiday_type_cd  --Ссылка на тип кредитных каникул
		        ,lh.loan_holiday_start_date     --Дата начала кредитных каникул
		        ,lh.loan_holiday_finish_date    --Дата окончания кредитных каникул
		        ,lh.loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
		        ,lh.loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
		        ,lh.loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
		        ,d.deal_name --Наименование сделки
		        ,d.deal_sum --Сумма сделки
		        ,d.client_rk --Ссылка на контрагента
		        ,d.agreement_rk --Ссылка на договор
		        ,d.deal_start_date --Дата начала действия сделки
		        ,d.department_rk --Ссылка на ГО/филиал
		        ,d.product_rk -- Ссылка на продукт
		        ,p.product_name -- Наименование продукта
		        ,d.deal_type_cd -- Наименование типа сделки
		FROM deal d
		LEFT JOIN loan_holiday lh ON 1=1
		                             AND d.deal_rk = lh.deal_rk
		                             AND d.effective_from_date = lh.effective_from_date
		LEFT JOIN product p ON p.product_rk = d.product_rk
							   AND p.effective_from_date = d.effective_from_date
		)
	INSERT INTO dm.loan_holiday_info (
        deal_rk,
        effective_from_date,
        effective_to_date,
        agreement_rk,
        client_rk,
        department_rk,
        product_rk,
        product_name,
        deal_type_cd,
        deal_start_date,
        deal_name,
        deal_number,
        deal_sum,
        loan_holiday_type_cd,
        loan_holiday_start_date,
        loan_holiday_finish_date,
        loan_holiday_fact_finish_date,
        loan_holiday_finish_flg,
        loan_holiday_last_possible_date
    )
	SELECT deal_rk
	      ,effective_from_date
	      ,effective_to_date
	      ,agreement_rk
	      ,client_rk
	      ,department_rk
	      ,product_rk
	      ,product_name
	      ,deal_type_cd
	      ,deal_start_date
	      ,deal_name
	      ,deal_number
	      ,deal_sum
	      ,loan_holiday_type_cd
	      ,loan_holiday_start_date
	      ,loan_holiday_finish_date
	      ,loan_holiday_fact_finish_date
	      ,loan_holiday_finish_flg
	      ,loan_holiday_last_possible_date
	FROM 
		holiday_info;

    RAISE NOTICE 'Перегрузка витрины завершена.';
END;
$$;

CALL reload_dm_loan_holiday_info();

-- В перегруженной витрине 10032 строки (без полных дубликатов)
SELECT * FROM dm.loan_holiday_info;

-- Комментарий: строки, которые различаются значением лишь в одном столбце (например, "Ипотека" / "Кредит наличными со страховкой" в 
-- product_name, я не удаляла, потому что нет возможности определить, и никак не указано, какое из этих значений является верным.
-- Полные дубликаты я решила удалить, чтобы не засорять витрину.
