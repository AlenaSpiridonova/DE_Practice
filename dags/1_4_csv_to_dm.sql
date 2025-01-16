-- Создание витрины для импорта данных из CSV-файла в витрину
CREATE TABLE dm.dm_f101_round_f_v2 (LIKE dm.dm_f101_round_f INCLUDING ALL);

-- Проверка
SELECT * FROM dm.dm_f101_round_f_v2;

SELECT * FROM dm.dm_f101_round_f;

SELECT * FROM logs.etl_logs;

TRUNCATE TABLE dm.dm_f101_round_f_v2 RESTART IDENTITY;

TRUNCATE TABLE logs.etl_logs RESTART IDENTITY;