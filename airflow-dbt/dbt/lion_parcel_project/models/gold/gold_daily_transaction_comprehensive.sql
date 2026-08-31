-- OPTIMASI: ORDER BY (Primary Index)
-- Mengurutkan berdasarkan tanggal dan status terlebih dahulu.
-- Jika dashboard memfilter "Berapa transaksi bulan ini?", kueri berjalan instan tanpa full scan.
{{
    config(
        materialized='table',
        schema='gold_lion',
        order_by=['transaction_date', 'last_status'],
        settings={'allow_nullable_key': 1}
    )
}}

-- ==============================================================================
-- GOLD LAYER: COMPREHENSIVE DAILY TRANSACTIONS
-- Tujuan: Rekapitulasi harian UTUH tanpa filter apa pun -- total_transactions
--         mencakup transaksi berjalan, batal (CANCELLED), maupun dihapus sistem.
-- Digunakan oleh: Manajemen Perusahaan (untuk laporan rekapitulasi harian utuh).
--
-- Cara membaca dua jenis anomali (bentuknya berbeda, keduanya sama-sama terhitung):
--   - Batal bisnis  -> BARIS ber-last_status = 'CANCELLED' (pembatalan mengubah status).
--   - Hapus sistem  -> KOLOM total_soft_deleted (soft delete TIDAK mengubah status,
--                      sehingga baris terhapus tetap membawa status aslinya).
-- ==============================================================================
SELECT
    toDate(created_at) AS transaction_date,
    last_status,
    pos_origin,
    pos_destination,
    count(id) AS total_transactions,
    -- Hanya menghitung hapus sistem (soft delete). Pembatalan bisnis TIDAK diikutkan
    -- di sini karena 'CANCELLED' sudah menjadi nilai pada grouping key last_status:
    -- rekap batal harian = filter baris WHERE last_status = 'CANCELLED'.
    countIf(is_deleted = 1) AS total_soft_deleted,
    now() AS processed_at
FROM {{ ref('silver_retail_transactions_cleansed') }}
GROUP BY
    transaction_date,
    last_status,
    pos_origin,
    pos_destination
