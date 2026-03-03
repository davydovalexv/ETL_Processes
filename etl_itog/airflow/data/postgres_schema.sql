CREATE TABLE IF NOT EXISTS user_sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    pages_visited TEXT[],
    device JSONB,
    actions TEXT[]
);

CREATE TABLE IF NOT EXISTS event_logs (
    event_id TEXT PRIMARY KEY,
    timestamp TIMESTAMP,
    event_type TEXT,
    details JSONB
);

CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id TEXT PRIMARY KEY,
    user_id TEXT,
    status TEXT,
    issue_type TEXT,
    messages JSONB,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id TEXT PRIMARY KEY,
    recommended_products TEXT[],
    last_updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS moderation_queue (
    review_id TEXT PRIMARY KEY,
    user_id TEXT,
    product_id TEXT,
    review_text TEXT,
    rating INTEGER,
    moderation_status TEXT,
    flags TEXT[],
    submitted_at TIMESTAMP
);

-- Analytical Views

CREATE OR REPLACE VIEW user_activity_summary AS
SELECT 
    user_id,
    COUNT(session_id) AS session_count,
    SUM(EXTRACT(EPOCH FROM end_time - start_time)) / 60 AS total_minutes,
    ARRAY_AGG(DISTINCT UNNEST(pages_visited)) AS unique_pages,
    ARRAY_AGG(DISTINCT UNNEST(actions)) AS unique_actions
FROM user_sessions
GROUP BY user_id;

CREATE OR REPLACE VIEW support_ticket_stats AS
SELECT
    issue_type,
    status,
    COUNT(ticket_id) AS num_tickets,
    AVG(EXTRACT(EPOCH FROM updated_at - created_at)) / 3600 AS avg_resolution_hours
FROM support_tickets
GROUP BY issue_type, status;