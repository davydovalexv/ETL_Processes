"""
JSON ETL Pipeline DAG for Apache Airflow
Extracts pets data from JSON source, transforms it, and loads to PostgreSQL
"""

from datetime import datetime, timedelta
import json
import logging
from typing import Dict, List, Any

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'etl_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# DAG definition
dag = DAG(
    'json_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for pets JSON data to PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'json', 'postgresql', 'pets'],
)

# Configuration
JSON_SOURCE_URL = "https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json"
POSTGRES_CONNECTION = "postgresql+psycopg2://airflow:airflow@postgres:5432/etl_data"
BATCH_SIZE = 100

logger = logging.getLogger(__name__)


def extract_data(**context) -> List[Dict[str, Any]]:
    """
    Extract pets data from JSON source URL
    """
    logger.info(f"Extracting data from {JSON_SOURCE_URL}")
    
    try:
        response = requests.get(JSON_SOURCE_URL, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract pets from the response
        pets = data.get('pets', [])
        
        logger.info(f"Extracted {len(pets)} pets")
        
        # Push to XCom for downstream tasks
        context['ti'].xcom_push(key='extracted_data', value=pets)
        
        return pets
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to extract data: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON: {e}")
        raise


def transform_data(**context) -> List[Dict[str, Any]]:
    """
    Transform extracted pets JSON data into structured format
    """
    logger.info("Transforming pets data")
    
    # Pull data from XCom
    pets = context['ti'].xcom_pull(task_ids='extract', key='extracted_data')
    
    if not pets:
        logger.warning("No data to transform")
        return []
    
    transformed_data = []
    current_year = datetime.now().year
    
    for pet in pets:
        try:
            # Extract and transform pet data
            name = pet.get('name', '').strip()
            species = pet.get('species', '').strip()
            birth_year = pet.get('birthYear')
            photo_url = pet.get('photo', '')
            
            # Handle favFoods array - convert to comma-separated string
            fav_foods = pet.get('favFoods', [])
            if isinstance(fav_foods, list):
                # Join array items and clean HTML tags if present
                fav_foods_str = ', '.join([str(food).replace('<strong>', '').replace('</strong>', '') 
                                          for food in fav_foods if food])
            else:
                fav_foods_str = str(fav_foods) if fav_foods else None
            
            # Calculate age and process birth year
            age = None
            birth_year_int = None
            if birth_year:
                try:
                    birth_year_int = int(birth_year)
                    age = current_year - birth_year_int
                except (ValueError, TypeError):
                    logger.warning(f"Invalid birth year for pet {name}: {birth_year}")
            
            # Validate required fields
            if not name or not species:
                logger.warning(f"Skipping pet with missing name or species: {pet}")
                continue
            
            transformed_pet = {
                'name': name,
                'species': species,
                'fav_foods': fav_foods_str,
                'birth_year': birth_year_int,
                'age': age,
                'photo_url': photo_url if photo_url else None,
            }
            
            transformed_data.append(transformed_pet)
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to transform pet {pet.get('name', 'unknown')}: {e}")
            continue
    
    logger.info(f"Transformed {len(transformed_data)} pets")
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='transformed_data', value=transformed_data)
    
    return transformed_data


def load_data(**context) -> Dict[str, Any]:
    """
    Load transformed data into PostgreSQL using SQLAlchemy 2.0
    """
    logger.info("Loading data to PostgreSQL")
    
    # Pull transformed data from XCom
    transformed_data = context['ti'].xcom_pull(task_ids='transform', key='transformed_data')
    
    if not transformed_data:
        logger.warning("No data to load")
        return {'loaded_count': 0, 'skipped_count': 0}
    
    try:
        # Create database connection with SQLAlchemy 2.0
        engine = create_engine(POSTGRES_CONNECTION, echo=False)
        
        # Convert to DataFrame for easier handling
        df = pd.DataFrame(transformed_data)
        
        # Track loading statistics
        loaded_count = 0
        skipped_count = 0
        
        # Load data in batches
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            
            try:
                # Use upsert to avoid duplicates
                with engine.begin() as conn:
                    for _, row in batch.iterrows():
                        row_dict = row.to_dict()
                        # Check if pet already exists (by name and species)
                        check_query = text("""
                            SELECT name FROM pets
                            WHERE name = :name AND species = :species
                        """)
                        result = conn.execute(
                            check_query,
                            {'name': row_dict['name'], 'species': row_dict['species']}
                        )
                        existing = result.fetchone()
                        
                        if existing:
                            # Update existing record
                            update_query = text("""
                                UPDATE pets
                                SET fav_foods = :fav_foods,
                                    birth_year = :birth_year,
                                    age = :age,
                                    photo_url = :photo_url
                                WHERE name = :name AND species = :species
                            """)
                            conn.execute(update_query, row_dict)
                            skipped_count += 1
                        else:
                            # Insert new record
                            insert_query = text("""
                                INSERT INTO pets
                                (name, species, fav_foods, birth_year, age, photo_url)
                                VALUES
                                (:name, :species, :fav_foods, :birth_year, :age, :photo_url)
                            """)
                            conn.execute(insert_query, row_dict)
                            loaded_count += 1
                    
            except SQLAlchemyError as e:
                logger.error(f"Failed to load batch {i}: {e}")
                raise
        
        logger.info(f"Loaded {loaded_count} new records, updated {skipped_count} existing records")
        
        # Log ETL run
        with engine.begin() as conn:
            log_query = text("""
                INSERT INTO etl_runs
                (dag_id, execution_date, status, records_processed)
                VALUES
                (:dag_id, :execution_date, :status, :records_processed)
            """)
            conn.execute(log_query, {
                'dag_id': 'json_etl_pipeline',
                'execution_date': datetime.now(),
                'status': 'SUCCESS',
                'records_processed': len(transformed_data)
            })
        
        return {
            'loaded_count': loaded_count,
            'skipped_count': skipped_count,
            'total_processed': len(transformed_data)
        }
        
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        
        # Log failed ETL run
        try:
            engine = create_engine(POSTGRES_CONNECTION, echo=False)
            with engine.begin() as conn:
                log_query = text("""
                    INSERT INTO etl_runs
                    (dag_id, execution_date, status, error_message)
                    VALUES
                    (:dag_id, :execution_date, :status, :error_message)
                """)
                conn.execute(log_query, {
                    'dag_id': 'json_etl_pipeline',
                    'execution_date': datetime.now(),
                    'status': 'FAILED',
                    'error_message': str(e)
                })
        except Exception as log_error:
            logger.error(f"Failed to log error: {log_error}")
        
        raise


def validate_data(**context) -> None:
    """
    Validate the loaded data using SQLAlchemy 2.0
    """
    logger.info("Validating loaded data")
    
    try:
        engine = create_engine(POSTGRES_CONNECTION, echo=False)
        
        with engine.connect() as conn:
            # Check total records
            count_query = text("SELECT COUNT(*) as total FROM pets")
            result = conn.execute(count_query)
            total_records = result.scalar() or 0
            
            # Check for duplicates (by name and species)
            duplicate_query = text("""
                SELECT name, species, COUNT(*) 
                FROM pets 
                GROUP BY name, species 
                HAVING COUNT(*) > 1
            """)
            duplicates_result = conn.execute(duplicate_query)
            duplicates = duplicates_result.fetchall()
            
            # Check data quality
            quality_query = text("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN name IS NULL OR name = '' THEN 1 ELSE 0 END) as missing_names,
                    SUM(CASE WHEN species IS NULL OR species = '' THEN 1 ELSE 0 END) as missing_species,
                    SUM(CASE WHEN birth_year IS NULL THEN 1 ELSE 0 END) as missing_birth_year,
                    SUM(CASE WHEN age IS NULL OR age < 0 THEN 1 ELSE 0 END) as invalid_age
                FROM pets
            """)
            quality_result = conn.execute(quality_query).fetchone()
            
            # Get species distribution
            species_query = text("""
                SELECT species, COUNT(*) as count
                FROM pets
                GROUP BY species
                ORDER BY count DESC
            """)
            species_result = conn.execute(species_query)
            species_dist = species_result.fetchall()
            
        logger.info(f"Validation results:")
        logger.info(f"  Total records: {total_records}")
        logger.info(f"  Duplicate pets (name+species): {len(duplicates)}")
        
        if quality_result:
            logger.info(f"  Missing names: {quality_result[1]}")
            logger.info(f"  Missing species: {quality_result[2]}")
            logger.info(f"  Missing birth year: {quality_result[3]}")
            logger.info(f"  Invalid age: {quality_result[4]}")
        
        logger.info(f"  Species distribution:")
        for species_row in species_dist:
            logger.info(f"    {species_row[0]}: {species_row[1]}")
        
        if len(duplicates) > 0:
            logger.warning(f"Found {len(duplicates)} duplicate pets (same name and species)")
            
    except SQLAlchemyError as e:
        logger.error(f"Validation error: {e}")
        raise


# Define DAG tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate',
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start_task >> extract_task >> transform_task >> load_task >> validate_task >> end_task