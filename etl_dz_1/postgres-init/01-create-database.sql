-- Create the ETL database if it doesn't exist
-- This script runs in the default postgres database context
SELECT 'CREATE DATABASE etl_data'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'etl_data')\gexec
