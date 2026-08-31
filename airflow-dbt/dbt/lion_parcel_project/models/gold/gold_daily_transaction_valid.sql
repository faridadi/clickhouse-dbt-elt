-- OPTIMASI: ORDER BY (Primary Index)
-- Memastikan pencarian laporan per tanggal tertentu tidak membebani IO disk.
{{
    config(
        materialized='table',
        schema='gold_lion',
        order_by=['transaction_date', 'last_status'],
        settings={'allow_nullable_key': 1}
    )
}}

-- ==============================================================================
-- GOLD LAYER: VALID DAILY TRANSACTIONS (CLEAN)
-- Tujuan: Menghitung total transaksi bersih (tanpa data batal/dihapus).
-- Digunakan oleh: Tim Keuangan & Operasional (sebagai dasar perhitungan revenue).
-- ==============================================================================
SELECT
    toDate(created_at) AS transaction_date,
    last_status,
    pos_origin,
    pos_destination,
    count(id) AS total_valid_transactions,
    now() AS processed_at
FROM {{ ref('silver_retail_transactions_cleansed') }}
WHERE is_deleted = 0 
  AND last_status != 'CANCELLED' -- LOGIC FIX: Transaksi batal tidak dihitung sebagai omzet/revenue valid
GROUP BY
    transaction_date,
    last_status,
    pos_origin,
    pos_destination
