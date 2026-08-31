-- ==============================================================================
-- TEST: KONSISTENSI FLAG SOFT DELETE
-- accepted_values pada is_deleted hanya menjamin nilainya 0 atau 1 -- flag yang
-- TERBALIK tetap lolos. Test ini mengunci maknanya ke sumbernya: is_deleted wajib
-- 1 tepat ketika deleted_at terisi. Melindungi Requirement R3 (sinkronisasi soft delete).
-- ==============================================================================

SELECT id, deleted_at, is_deleted
FROM {{ ref('silver_retail_transactions_cleansed') }}
WHERE (deleted_at IS NOT NULL AND is_deleted != 1)
   OR (deleted_at IS NULL     AND is_deleted != 0)
