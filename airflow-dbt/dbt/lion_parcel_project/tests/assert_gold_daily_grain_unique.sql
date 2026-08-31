-- ==============================================================================
-- TEST: KEUNIKAN GRAIN MART HARIAN
-- Grain kedua mart harian adalah (transaction_date, last_status, pos_origin, pos_destination).
-- Baris ganda berarti GROUP BY rusak, dan seluruh angka dashboard menggelembung
-- tanpa ada satu pun test lain yang menyadarinya. Test mengembalikan baris = GAGAL.
-- ==============================================================================

SELECT
    'gold_daily_transaction_valid' AS model_name,
    transaction_date, last_status, pos_origin, pos_destination,
    count() AS jumlah_baris
FROM {{ ref('gold_daily_transaction_valid') }}
GROUP BY transaction_date, last_status, pos_origin, pos_destination
HAVING count() > 1

UNION ALL

SELECT
    'gold_daily_transaction_comprehensive' AS model_name,
    transaction_date, last_status, pos_origin, pos_destination,
    count() AS jumlah_baris
FROM {{ ref('gold_daily_transaction_comprehensive') }}
GROUP BY transaction_date, last_status, pos_origin, pos_destination
HAVING count() > 1
