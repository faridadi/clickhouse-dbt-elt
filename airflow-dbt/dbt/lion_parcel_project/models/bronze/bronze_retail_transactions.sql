-- OPTIMASI 1: ORDER BY (Primary Index)
-- Urutan kolom: (1) Waktu, (2) Kolom Filter, (3) Unique ID.
-- Mempercepat kueri analitik hingga 100x lipat (Index Pruning).
-- OPTIMASI 2: PARTITION BY (Manajemen Storage)
-- Partisi per bulan agar penghapusan data lama (TTL) bisa instan tanpa membebani server.
{{
    config(
        materialized='incremental',
        schema='bronze_lion',
        engine='ReplacingMergeTree(updatedAt)',
        unique_key='id',
        order_by=['toDate(updatedAt)', 'lastStatus', 'id'],
        partition_by=['toYYYYMM(createdAt)'],
        settings={'allow_nullable_key': 1}
    )
}}

-- ==============================================================================
-- BRONZE LAYER: RAW RETAIL TRANSACTIONS
-- Tujuan: Mengingesti data mentah (raw) dari PostgreSQL ke ClickHouse secara persis.
-- Digunakan oleh: Data Engineer (sebagai fondasi data historis mentah).
--
-- ARCHITECTURE NOTE (SMART BRONZE):
-- Meskipun ini adalah Bronze Layer (1:1 dengan source), penerapan optimasi fisik
-- seperti LowCardinality, PARTITION BY, dan ORDER BY SANGAT DIANJURKAN di ClickHouse.
-- Ini tidak menyalahi aturan "Raw Data" karena kita TIDAK mengubah makna/logika bisnis data, 
-- melainkan hanya mengatur kompresi fisik di disk agar pipeline dbt selanjutnya 
-- terhindar dari Full Table Scan dan hemat storage.
-- ==============================================================================
SELECT 
    id,
    "customerId",
    
    -- OPTIMASI 3: LowCardinality (Kompresi Teks)
    -- Menyimpan teks berulang (seperti nama kota/status) sebagai angka referensi di RAM.
    -- Menghemat storage disk dan membuat agregasi GROUP BY secepat kilat.
    CAST("lastStatus" AS LowCardinality(Nullable(String))) AS lastStatus,
    CAST("posOrigin" AS LowCardinality(Nullable(String))) AS posOrigin,
    CAST("posDestination" AS LowCardinality(Nullable(String))) AS posDestination,
    
    "createdAt",
    assumeNotNull("updatedAt") AS updatedAt,
    "deletedAt",
    now() AS load_at
FROM {{ source('postgres_source', 'retail_transactions') }}

{% if is_incremental() %}
    -- Menggunakan >= agar transaksi di milidetik yang sama persis tidak terlewat (aman dari duplikasi karena ReplacingMergeTree)
    WHERE "updatedAt" >= (SELECT max(updatedAt) FROM {{ this }})
{% endif %}
