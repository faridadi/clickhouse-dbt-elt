{{
    config(
        materialized='view',
        schema='silver_lion'
    )
}}

-- ==============================================================================
-- SILVER LAYER: CLEANSED RETAIL TRANSACTIONS
-- Tujuan: Membersihkan dan menstandarkan format data (snake_case, flag boolean).
-- Digunakan oleh: Data Analyst & Data Scientist (sebagai Single Source of Truth).
-- ==============================================================================

SELECT
    trim(id) AS id,
    trim("customerId") AS customer_id,
    -- Memastikan tidak ada spasi tersembunyi & selalu UPPERCASE
    upper(trim("lastStatus")) AS last_status,
    trim("posOrigin") AS pos_origin,
    trim("posDestination") AS pos_destination,
    "createdAt" AS created_at,
    "updatedAt" AS updated_at,
    "deletedAt" AS deleted_at,
    load_at,
    
    -- Pembuatan flag boolean untuk mempermudah analis membedakan transaksi aktif/terhapus
    IF("deletedAt" IS NOT NULL, 1, 0) AS is_deleted
    
FROM {{ ref('bronze_retail_transactions') }}
