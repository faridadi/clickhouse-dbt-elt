-- ==============================================================================
-- TEST: PERSENTASE BERADA DI RENTANG WAJAR
-- Menjaga dua invarian: (1) tiap rasio berada di 0-100, (2) sukses + batal tidak
-- melebihi 100 karena DELIVERED dan CANCELLED saling eksklusif dan berbagi penyebut
-- yang sama. Toleransi 0.01 untuk pembulatan float.
-- ==============================================================================

SELECT pos_origin, pos_destination, success_rate_pct, cancelled_rate_pct
FROM {{ ref('gold_delivery_success_rate') }}
WHERE success_rate_pct   < 0 OR success_rate_pct   > 100
   OR cancelled_rate_pct < 0 OR cancelled_rate_pct > 100
   OR (success_rate_pct + cancelled_rate_pct) > 100.01
