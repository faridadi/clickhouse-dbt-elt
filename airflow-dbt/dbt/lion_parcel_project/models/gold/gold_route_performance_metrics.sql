-- OPTIMASI: ORDER BY (Primary Index)
-- Mengurutkan berdasarkan rute origin-destination agar filter rute berjalan kilat
{{
    config(
        materialized='table',
        schema='gold_lion',
        order_by=['pos_origin', 'pos_destination'],
        settings={'allow_nullable_key': 1}
    )
}}

-- ==============================================================================
-- 1. ROUTE PERFORMANCE & SLA METRICS
-- Tujuan: Mengukur kinerja pengiriman antar kota (End-to-End Lead Time).
-- Digunakan oleh: Direktur Operasional untuk menilai efisiensi logistik per rute.
-- ==============================================================================

SELECT
    pos_origin,
    pos_destination,
    count(id) AS total_delivered_packages,
    
    -- Menghitung durasi (dalam jam) dari paket pertama kali dibuat hingga tiba (DELIVERED)
    round(avg(dateDiff('hour', created_at, updated_at)), 2) AS avg_delivery_time_hours,
    min(dateDiff('hour', created_at, updated_at)) AS fastest_delivery_hours,
    max(dateDiff('hour', created_at, updated_at)) AS slowest_delivery_hours,
    
    now() AS processed_at

FROM {{ ref('silver_retail_transactions_cleansed') }}
WHERE last_status = 'DELIVERED'
  AND is_deleted = 0
GROUP BY
    pos_origin,
    pos_destination
ORDER BY
    total_delivered_packages DESC
