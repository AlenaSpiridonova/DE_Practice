-- Удаление дублей из таблицы
WITH duplicates AS (
    SELECT 
        ctid,  -- системный идентификатор строки, идентифицирующий физическое расположение строки в таблице
        ROW_NUMBER() OVER (
            PARTITION BY client_rk, effective_from_date 
            ORDER BY effective_to_date DESC, client_open_dttm DESC -- отбор самых "свежих" записей на случай различия между ними
        ) AS row_num
    FROM 
        dm.client
)
DELETE FROM dm.client
WHERE ctid IN (
    SELECT ctid 
    FROM duplicates 
    WHERE row_num > 1
);


-- Для проверки данных
-- Отбор первичных ключей уникальных строк для проверки
SELECT 
	client_rk, 
	effective_from_date
FROM 
	dm.client
GROUP BY 
	client_rk, 
	effective_from_date;

-- Нахождение дублей и количества повторов каждого из них
SELECT 
    client_rk, 
    effective_from_date, 
    COUNT(*) AS dupl_count
FROM 
    dm.client
GROUP BY 
    client_rk, 
    effective_from_date
HAVING 
    COUNT(*) > 1;

