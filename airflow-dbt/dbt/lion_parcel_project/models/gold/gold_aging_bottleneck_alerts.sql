-- OPTIMASI: ORDER BY (Primary Index)
{{
    config(
        materialized='table',
        schema='gold_lion',
        order_by=['last_status', 'pos_origin'],
        settings={'allow_nullable_key': 1}
    )
}}

-- ==============================================================================
-- 2. AGING & BOTTLENECK ALERTS (RADAR PENUMPUKAN)
-- Tujuan: Mendeteksi anomali paket yang berhenti bergerak (stuck).
-- Digunakan oleh: Tim Gudang & Customer Service untuk eskalasi keterlambatan.
-- ==============================================================================

SELECT
    last_status,
    pos_origin,
    pos_destination,
    count(id) AS total_stuck_packages,
    
    -- Menghitung sudah berapa jam paket ini tidak mengalami perubahan status sejak update terakhir
    round(avg(dateDiff('hour', updated_at, now())), 2) AS avg_hours_stuck,
    max(dateDiff('hour', updated_at, now())) AS max_hours_stuck,
    
    now() AS processed_at

FROM {{ ref('silver_retail_transactions_cleansed') }}
WHERE last_status NOT IN ('DELIVERED', 'CANCELLED')
  AND is_deleted = 0
  -- Flagging kritis: Hanya tampilkan jalur yang memiliki paket stuck lebih dari 12 jam
  AND dateDiff('hour', updated_at, now()) >= 12
GROUP BY
    last_status,
    pos_origin,
    pos_destination
ORDER BY
    total_stuck_packages DESC,
    avg_hours_stuck DESC
