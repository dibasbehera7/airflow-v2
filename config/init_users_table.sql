-- Create the users source table for the postgres-to-opensearch DAG
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed users dynamically (1,000 records) if empty
INSERT INTO users (name, email, status, created_at)
SELECT 
    'User ' || i,
    'user.' || i || '@example.com',
    CASE WHEN i % 10 = 0 THEN 'inactive' WHEN i % 15 = 0 THEN 'suspended' ELSE 'active' END,
    NOW() - (i || ' minutes')::interval
FROM generate_series(1, 1000) AS i
ON CONFLICT (email) DO NOTHING;

-- Create the addresses source table
CREATE TABLE IF NOT EXISTS addresses (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    type VARCHAR(50) NOT NULL,
    address VARCHAR(255) NOT NULL,
    long DOUBLE PRECISION,
    lati DOUBLE PRECISION
);

-- Seed addresses dynamically (2 or 3 per user) rotating through 10 Indian cities if empty
INSERT INTO addresses (user_id, type, address, long, lati)
SELECT 
    u.id,
    addr.type,
    (ARRAY[
        'mumbai - 400001',
        'bangalore - 560032',
        'delhi - 110001',
        'chennai - 600001',
        'hyderabad - 500001',
        'kolkata - 700001',
        'pune - 411001',
        'ahmedabad - 380001',
        'jaipur - 302001',
        'lucknow - 226001'
    ])[1 + ((u.id + addr.idx) % 10)],
    (ARRAY[72.8777, 77.5946, 77.2090, 80.2707, 78.4867, 88.3639, 73.8567, 72.5714, 75.7873, 80.9462])[1 + ((u.id + addr.idx) % 10)] + ((u.id + addr.idx) % 50) * 0.001,
    (ARRAY[19.0760, 12.9716, 28.6139, 13.0827, 17.3850, 22.5726, 18.5204, 23.0225, 26.9124, 26.8467])[1 + ((u.id + addr.idx) % 10)] + ((u.id + addr.idx) % 50) * 0.001
FROM users u
CROSS JOIN LATERAL (
    SELECT 'permanent' AS type, 1 AS idx
    UNION ALL
    SELECT 'office' AS type, 2 AS idx
    UNION ALL
    -- Only generate a 3rd address ('temporary') for users where u.id % 2 = 0
    SELECT 'temporary' AS type, 3 AS idx
    WHERE u.id % 2 = 0
) AS addr
WHERE NOT EXISTS (SELECT 1 FROM addresses LIMIT 1);
