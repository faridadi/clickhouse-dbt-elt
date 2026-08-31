-- 1. Buat Tabel (Menggunakan camelCase sebagai sumber mentah)
CREATE TABLE retail_transactions (
    "id" VARCHAR(50) PRIMARY KEY,             -- Receipt ID
    "customerId" VARCHAR(50),
    "lastStatus" VARCHAR(50),                -- 'BOOKED', 'PROCESSING', 'IN_TRANSIT', 'OUT_FOR_DELIVERY', 'DELIVERED', 'CANCELLED'
    "posOrigin" VARCHAR(100),
    "posDestination" VARCHAR(100),
    "createdAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Diisi otomatis saat record dibuat
    "updatedAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Diisi otomatis saat record dibuat/update
    "deletedAt" TIMESTAMPTZ NULL               -- Diisi jika transaksi di-soft delete
);

-- 2. Buat Fungsi (Function) untuk update waktu
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW."updatedAt" = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 3. Pasang Trigger pada Tabel
CREATE TRIGGER trigger_update_timestamp
BEFORE UPDATE ON retail_transactions
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- 4. Insert 100 Data Awal (Seed Data) dengan status terbaru
INSERT INTO retail_transactions ("id", "customerId", "lastStatus", "posOrigin", "posDestination")
VALUES 
    ('RCPT-INIT-001', 'CUST-754', 'BOOKED', 'Jakarta', 'Makassar'),
    ('RCPT-INIT-002', 'CUST-381', 'PROCESSING', 'Medan', 'Bandung'),
    ('RCPT-INIT-003', 'CUST-854', 'BOOKED', 'Bandung', 'Semarang'),
    ('RCPT-INIT-004', 'CUST-532', 'BOOKED', 'Jakarta', 'Bandung'),
    ('RCPT-INIT-005', 'CUST-323', 'PROCESSING', 'Jakarta', 'Semarang'),
    ('RCPT-INIT-006', 'CUST-303', 'CANCELLED', 'Makassar', 'Bandung'),
    ('RCPT-INIT-007', 'CUST-559', 'DELIVERED', 'Denpasar', 'Palembang'),
    ('RCPT-INIT-008', 'CUST-990', 'BOOKED', 'Surabaya', 'Makassar'),
    ('RCPT-INIT-009', 'CUST-532', 'IN_TRANSIT', 'Denpasar', 'Bandung'),
    ('RCPT-INIT-010', 'CUST-320', 'IN_TRANSIT', 'Bandung', 'Jakarta'),
    ('RCPT-INIT-011', 'CUST-489', 'BOOKED', 'Semarang', 'Palembang'),
    ('RCPT-INIT-012', 'CUST-452', 'DELIVERED', 'Denpasar', 'Palembang'),
    ('RCPT-INIT-013', 'CUST-144', 'CANCELLED', 'Palembang', 'Denpasar'),
    ('RCPT-INIT-014', 'CUST-227', 'OUT_FOR_DELIVERY', 'Bandung', 'Semarang'),
    ('RCPT-INIT-015', 'CUST-400', 'CANCELLED', 'Semarang', 'Denpasar'),
    ('RCPT-INIT-016', 'CUST-296', 'CANCELLED', 'Bandung', 'Jakarta'),
    ('RCPT-INIT-017', 'CUST-777', 'PROCESSING', 'Denpasar', 'Jakarta'),
    ('RCPT-INIT-018', 'CUST-975', 'PROCESSING', 'Bandung', 'Denpasar'),
    ('RCPT-INIT-019', 'CUST-384', 'OUT_FOR_DELIVERY', 'Semarang', 'Bandung'),
    ('RCPT-INIT-020', 'CUST-479', 'IN_TRANSIT', 'Medan', 'Makassar'),
    ('RCPT-INIT-021', 'CUST-373', 'CANCELLED', 'Bandung', 'Semarang'),
    ('RCPT-INIT-022', 'CUST-750', 'PROCESSING', 'Medan', 'Bandung'),
    ('RCPT-INIT-023', 'CUST-573', 'OUT_FOR_DELIVERY', 'Denpasar', 'Makassar'),
    ('RCPT-INIT-024', 'CUST-804', 'DELIVERED', 'Medan', 'Makassar'),
    ('RCPT-INIT-025', 'CUST-432', 'BOOKED', 'Medan', 'Palembang'),
    ('RCPT-INIT-026', 'CUST-132', 'IN_TRANSIT', 'Makassar', 'Surabaya'),
    ('RCPT-INIT-027', 'CUST-167', 'PROCESSING', 'Semarang', 'Bandung'),
    ('RCPT-INIT-028', 'CUST-771', 'OUT_FOR_DELIVERY', 'Makassar', 'Semarang'),
    ('RCPT-INIT-029', 'CUST-569', 'PROCESSING', 'Denpasar', 'Bandung'),
    ('RCPT-INIT-030', 'CUST-352', 'CANCELLED', 'Denpasar', 'Makassar'),
    ('RCPT-INIT-031', 'CUST-698', 'OUT_FOR_DELIVERY', 'Makassar', 'Surabaya'),
    ('RCPT-INIT-032', 'CUST-324', 'PROCESSING', 'Palembang', 'Jakarta'),
    ('RCPT-INIT-033', 'CUST-873', 'BOOKED', 'Bandung', 'Surabaya'),
    ('RCPT-INIT-034', 'CUST-742', 'PROCESSING', 'Makassar', 'Denpasar'),
    ('RCPT-INIT-035', 'CUST-165', 'OUT_FOR_DELIVERY', 'Makassar', 'Denpasar'),
    ('RCPT-INIT-036', 'CUST-579', 'DELIVERED', 'Denpasar', 'Semarang'),
    ('RCPT-INIT-037', 'CUST-981', 'BOOKED', 'Bandung', 'Makassar'),
    ('RCPT-INIT-038', 'CUST-649', 'IN_TRANSIT', 'Semarang', 'Jakarta'),
    ('RCPT-INIT-039', 'CUST-400', 'OUT_FOR_DELIVERY', 'Surabaya', 'Denpasar'),
    ('RCPT-INIT-040', 'CUST-103', 'CANCELLED', 'Denpasar', 'Semarang'),
    ('RCPT-INIT-041', 'CUST-880', 'PROCESSING', 'Bandung', 'Palembang'),
    ('RCPT-INIT-042', 'CUST-740', 'IN_TRANSIT', 'Medan', 'Bandung'),
    ('RCPT-INIT-043', 'CUST-482', 'PROCESSING', 'Jakarta', 'Semarang'),
    ('RCPT-INIT-044', 'CUST-431', 'OUT_FOR_DELIVERY', 'Jakarta', 'Bandung'),
    ('RCPT-INIT-045', 'CUST-471', 'IN_TRANSIT', 'Medan', 'Jakarta'),
    ('RCPT-INIT-046', 'CUST-346', 'DELIVERED', 'Bandung', 'Jakarta'),
    ('RCPT-INIT-047', 'CUST-849', 'OUT_FOR_DELIVERY', 'Bandung', 'Palembang'),
    ('RCPT-INIT-048', 'CUST-645', 'PROCESSING', 'Surabaya', 'Makassar'),
    ('RCPT-INIT-049', 'CUST-586', 'DELIVERED', 'Surabaya', 'Medan'),
    ('RCPT-INIT-050', 'CUST-640', 'DELIVERED', 'Makassar', 'Bandung'),
    ('RCPT-INIT-051', 'CUST-652', 'CANCELLED', 'Medan', 'Makassar'),
    ('RCPT-INIT-052', 'CUST-419', 'OUT_FOR_DELIVERY', 'Semarang', 'Medan'),
    ('RCPT-INIT-053', 'CUST-629', 'OUT_FOR_DELIVERY', 'Bandung', 'Surabaya'),
    ('RCPT-INIT-054', 'CUST-330', 'BOOKED', 'Semarang', 'Jakarta'),
    ('RCPT-INIT-055', 'CUST-702', 'DELIVERED', 'Medan', 'Semarang'),
    ('RCPT-INIT-056', 'CUST-325', 'BOOKED', 'Bandung', 'Makassar'),
    ('RCPT-INIT-057', 'CUST-746', 'BOOKED', 'Medan', 'Jakarta'),
    ('RCPT-INIT-058', 'CUST-132', 'IN_TRANSIT', 'Bandung', 'Semarang'),
    ('RCPT-INIT-059', 'CUST-343', 'IN_TRANSIT', 'Palembang', 'Bandung'),
    ('RCPT-INIT-060', 'CUST-652', 'PROCESSING', 'Palembang', 'Bandung'),
    ('RCPT-INIT-061', 'CUST-903', 'OUT_FOR_DELIVERY', 'Makassar', 'Bandung'),
    ('RCPT-INIT-062', 'CUST-196', 'BOOKED', 'Makassar', 'Surabaya'),
    ('RCPT-INIT-063', 'CUST-533', 'OUT_FOR_DELIVERY', 'Palembang', 'Makassar'),
    ('RCPT-INIT-064', 'CUST-846', 'BOOKED', 'Bandung', 'Jakarta'),
    ('RCPT-INIT-065', 'CUST-512', 'CANCELLED', 'Semarang', 'Palembang'),
    ('RCPT-INIT-066', 'CUST-982', 'BOOKED', 'Medan', 'Bandung'),
    ('RCPT-INIT-067', 'CUST-294', 'DELIVERED', 'Palembang', 'Bandung'),
    ('RCPT-INIT-068', 'CUST-532', 'PROCESSING', 'Denpasar', 'Medan'),
    ('RCPT-INIT-069', 'CUST-355', 'BOOKED', 'Palembang', 'Makassar'),
    ('RCPT-INIT-070', 'CUST-982', 'DELIVERED', 'Bandung', 'Jakarta'),
    ('RCPT-INIT-071', 'CUST-767', 'DELIVERED', 'Jakarta', 'Bandung'),
    ('RCPT-INIT-072', 'CUST-871', 'PROCESSING', 'Surabaya', 'Denpasar'),
    ('RCPT-INIT-073', 'CUST-597', 'OUT_FOR_DELIVERY', 'Medan', 'Palembang'),
    ('RCPT-INIT-074', 'CUST-510', 'BOOKED', 'Surabaya', 'Denpasar'),
    ('RCPT-INIT-075', 'CUST-102', 'OUT_FOR_DELIVERY', 'Denpasar', 'Palembang'),
    ('RCPT-INIT-076', 'CUST-903', 'OUT_FOR_DELIVERY', 'Denpasar', 'Medan'),
    ('RCPT-INIT-077', 'CUST-813', 'CANCELLED', 'Palembang', 'Bandung'),
    ('RCPT-INIT-078', 'CUST-294', 'IN_TRANSIT', 'Medan', 'Jakarta'),
    ('RCPT-INIT-079', 'CUST-693', 'CANCELLED', 'Jakarta', 'Makassar'),
    ('RCPT-INIT-080', 'CUST-421', 'BOOKED', 'Jakarta', 'Semarang'),
    ('RCPT-INIT-081', 'CUST-588', 'DELIVERED', 'Surabaya', 'Jakarta'),
    ('RCPT-INIT-082', 'CUST-620', 'BOOKED', 'Surabaya', 'Jakarta'),
    ('RCPT-INIT-083', 'CUST-709', 'BOOKED', 'Medan', 'Denpasar'),
    ('RCPT-INIT-084', 'CUST-222', 'DELIVERED', 'Medan', 'Semarang'),
    ('RCPT-INIT-085', 'CUST-708', 'BOOKED', 'Bandung', 'Denpasar'),
    ('RCPT-INIT-086', 'CUST-773', 'DELIVERED', 'Semarang', 'Surabaya'),
    ('RCPT-INIT-087', 'CUST-309', 'CANCELLED', 'Semarang', 'Bandung'),
    ('RCPT-INIT-088', 'CUST-371', 'OUT_FOR_DELIVERY', 'Surabaya', 'Makassar'),
    ('RCPT-INIT-089', 'CUST-760', 'IN_TRANSIT', 'Palembang', 'Surabaya'),
    ('RCPT-INIT-090', 'CUST-869', 'BOOKED', 'Jakarta', 'Denpasar'),
    ('RCPT-INIT-091', 'CUST-736', 'DELIVERED', 'Bandung', 'Jakarta'),
    ('RCPT-INIT-092', 'CUST-650', 'PROCESSING', 'Denpasar', 'Bandung'),
    ('RCPT-INIT-093', 'CUST-457', 'BOOKED', 'Medan', 'Surabaya'),
    ('RCPT-INIT-094', 'CUST-391', 'PROCESSING', 'Palembang', 'Makassar'),
    ('RCPT-INIT-095', 'CUST-656', 'CANCELLED', 'Denpasar', 'Semarang'),
    ('RCPT-INIT-096', 'CUST-926', 'CANCELLED', 'Jakarta', 'Makassar'),
    ('RCPT-INIT-097', 'CUST-936', 'DELIVERED', 'Denpasar', 'Makassar'),
    ('RCPT-INIT-098', 'CUST-206', 'PROCESSING', 'Denpasar', 'Jakarta'),
    ('RCPT-INIT-099', 'CUST-209', 'CANCELLED', 'Surabaya', 'Medan'),
    ('RCPT-INIT-100', 'CUST-388', 'DELIVERED', 'Medan', 'Makassar');
