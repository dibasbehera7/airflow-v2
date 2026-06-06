-- Create the users source table for the postgres-to-opensearch DAG
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed with sample data (only if table is empty)
INSERT INTO users (name, email, status, created_at)
SELECT name, email, status, created_at
FROM (VALUES
    ('Alice Johnson',   'alice.johnson@example.com',   'active',   '2024-01-15 09:30:00'::timestamp),
    ('Bob Smith',       'bob.smith@example.com',       'active',   '2024-02-20 14:15:00'::timestamp),
    ('Carol Williams',  'carol.williams@example.com',  'inactive', '2024-03-10 11:45:00'::timestamp),
    ('David Brown',     'david.brown@example.com',     'active',   '2024-04-05 16:20:00'::timestamp),
    ('Eve Davis',       'eve.davis@example.com',       'active',   '2024-05-12 08:00:00'::timestamp),
    ('Frank Miller',    'frank.miller@example.com',    'suspended','2024-06-01 10:30:00'::timestamp),
    ('Grace Wilson',    'grace.wilson@example.com',    'active',   '2024-07-22 13:45:00'::timestamp),
    ('Henry Moore',     'henry.moore@example.com',     'inactive', '2024-08-18 17:00:00'::timestamp),
    ('Ivy Taylor',      'ivy.taylor@example.com',      'active',   '2024-09-03 09:15:00'::timestamp),
    ('Jack Anderson',   'jack.anderson@example.com',   'active',   '2024-10-28 12:30:00'::timestamp)
) AS seed(name, email, status, created_at)
WHERE NOT EXISTS (SELECT 1 FROM users LIMIT 1);
