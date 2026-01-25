#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
until psql -U "$POSTGRES_USER" -d postgres -c '\q' 2>/dev/null; do
  >&2 echo "PostgreSQL is unavailable - sleeping"
  sleep 1
done

# Ensure etl_data database exists (in case 01-create-database.sql hasn't run yet)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    SELECT 'CREATE DATABASE etl_data'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'etl_data')\gexec
EOSQL

# Create tables in etl_data database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "etl_data" <<-EOSQL
    -- Create table for storing extracted and transformed pets data
    CREATE TABLE IF NOT EXISTS pets (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        species VARCHAR(50) NOT NULL,
        fav_foods TEXT,
        birth_year INTEGER,
        age INTEGER,
        photo_url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT unique_pet_name_species UNIQUE (name, species)
    );

    -- Create index for better query performance
    CREATE INDEX IF NOT EXISTS idx_pets_species ON pets(species);
    CREATE INDEX IF NOT EXISTS idx_pets_name ON pets(name);
    CREATE INDEX IF NOT EXISTS idx_pets_birth_year ON pets(birth_year);

    -- Create function to update updated_at timestamp
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS \$\$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    \$\$ language 'plpgsql';

    -- Create trigger to automatically update updated_at
    DROP TRIGGER IF EXISTS update_pets_updated_at ON pets;
    CREATE TRIGGER update_pets_updated_at
        BEFORE UPDATE ON pets
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();

    -- Create table for tracking ETL runs
    CREATE TABLE IF NOT EXISTS etl_runs (
        run_id SERIAL PRIMARY KEY,
        dag_id VARCHAR(100) NOT NULL,
        execution_date TIMESTAMP NOT NULL,
        status VARCHAR(20) NOT NULL,
        records_processed INTEGER,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP,
        error_message TEXT
    );

    -- Create index for etl_runs queries
    CREATE INDEX IF NOT EXISTS idx_etl_runs_dag_id ON etl_runs(dag_id);
    CREATE INDEX IF NOT EXISTS idx_etl_runs_execution_date ON etl_runs(execution_date);
    CREATE INDEX IF NOT EXISTS idx_etl_runs_status ON etl_runs(status);

    -- Create table for storing nutrition/food data from XML
    CREATE TABLE IF NOT EXISTS nutrition_foods (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        manufacturer VARCHAR(255),
        serving_value VARCHAR(50),
        serving_units VARCHAR(50),
        calories_total DECIMAL(10, 2),
        calories_fat DECIMAL(10, 2),
        total_fat DECIMAL(10, 2),
        saturated_fat DECIMAL(10, 2),
        cholesterol DECIMAL(10, 2),
        sodium DECIMAL(10, 2),
        carb DECIMAL(10, 2),
        fiber DECIMAL(10, 2),
        protein DECIMAL(10, 2),
        vitamin_a DECIMAL(10, 2),
        vitamin_c DECIMAL(10, 2),
        calcium DECIMAL(10, 2),
        iron DECIMAL(10, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Create index for better query performance
    CREATE INDEX IF NOT EXISTS idx_nutrition_foods_name ON nutrition_foods(name);
    CREATE INDEX IF NOT EXISTS idx_nutrition_foods_manufacturer ON nutrition_foods(manufacturer);
    CREATE INDEX IF NOT EXISTS idx_nutrition_foods_calories ON nutrition_foods(calories_total);

    -- Create trigger to automatically update updated_at for nutrition_foods
    DROP TRIGGER IF EXISTS update_nutrition_foods_updated_at ON nutrition_foods;
    CREATE TRIGGER update_nutrition_foods_updated_at
        BEFORE UPDATE ON nutrition_foods
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
EOSQL
