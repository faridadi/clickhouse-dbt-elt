-- ==============================================================================
-- TEST: LEAD TIME TIDAK MUNGKIN NEGATIF
-- Lead time negatif berarti updated_at mendahului created_at: jam sumber mundur,
-- atau kolom tertukar saat refactor. not_null tidak menangkap ini.
-- ==============================================================================

SELECT pos_origin, pos_destination,
       fastest_delivery_hours, avg_delivery_time_hours, slowest_delivery_hours
FROM {{ ref('gold_route_performance_metrics') }}
WHERE fastest_delivery_hours  < 0
   OR avg_delivery_time_hours < 0
   OR slowest_delivery_hours  < fastest_delivery_hours
