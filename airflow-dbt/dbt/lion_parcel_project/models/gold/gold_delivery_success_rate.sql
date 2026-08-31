-- OPTIMASI: ORDER BY (Primary Index)
{{
    config(
        materialized='table',
        schema='gold_lion',
        order_by=['pos_origin', 'pos_destination'],
        settings={'allow_nullable_key': 1}
    )
}}

-- ==============================================================================
-- 3. DELIVERY SUCCESS & VOID RATE
-- Tujuan: Menghitung persentase keberhasilan (Success Rate) vs Pembatalan.
-- Digunakan oleh: Manajemen Perusahaan untuk audit performa rute / cabang.
-- ==============================================================================

SELECT
    pos_origin,
    pos_destination,
    count(id) AS total_packages,
    
    countIf(last_status = 'DELIVERED') AS total_delivered,
    countIf(last_status = 'CANCELLED') AS total_cancelled,
    
    -- Rasio Persentase Keberhasilan Pengiriman (Murni transaksi sah)
    round((countIf(last_status = 'DELIVERED') / count(id)) * 100, 2) AS success_rate_pct,
    
    -- Rasio Persentase Kegagalan / Pembatalan (Murni transaksi sah)
    round((countIf(last_status = 'CANCELLED') / count(id)) * 100, 2) AS cancelled_rate_pct,
    
    now() AS processed_at

FROM {{ ref('silver_retail_transactions_cleansed') }}
WHERE is_deleted = 0  -- LOGIC FIX: Jangan hitung paket salah input/soft-delete
GROUP BY
    pos_origin,
    pos_destination
HAVING total_packages > 0
ORDER BY
    total_packages DESC
