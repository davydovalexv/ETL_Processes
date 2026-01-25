"""
XML to SQL Pipeline DAG for Apache Airflow
Extracts data from XML source, transforms it, and loads to PostgreSQL
"""

from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any
import xml.etree.ElementTree as ET

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
    'xml_to_sql_pipeline',
    default_args=default_args,
    description='ETL pipeline for XML nutrition data to PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'xml', 'postgresql', 'nutrition'],
)

# Configuration
XML_SOURCE_URL = "https://gist.githubusercontent.com/pamelafox/3000322/raw/nutrition.xml"
POSTGRES_CONNECTION = "postgresql+psycopg2://airflow:airflow@postgres:5432/etl_data"
BATCH_SIZE = 100

logger = logging.getLogger(__name__)


def extract_xml_data(**context) -> str:
    """
    Extract XML data from source URL
    """
    logger.info(f"Extracting XML data from {XML_SOURCE_URL}")
    
    try:
        response = requests.get(XML_SOURCE_URL, timeout=30)
        response.raise_for_status()
        
        # Parse XML content to validate it
        root = ET.fromstring(response.content)
        
        # Get XML as string for XCom (must be JSON serializable)
        xml_string = ET.tostring(root, encoding='unicode')
        
        logger.info(f"Extracted XML data, root element: {root.tag}")
        logger.info(f"XML content length: {len(xml_string)} characters")
        
        # Push to XCom for downstream tasks
        context['ti'].xcom_push(key='xml_root', value=xml_string)
        
        # Return string instead of Element object (must be JSON serializable)
        return xml_string
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to extract XML data: {e}")
        raise
    except ET.ParseError as e:
        logger.error(f"Failed to parse XML: {e}")
        raise


def transform_xml_data(**context) -> List[Dict[str, Any]]:
    """
    Transform XML data into structured format for database
    """
    logger.info("Transforming XML data")
    
    # Pull XML data from XCom
    xml_string = context['ti'].xcom_pull(task_ids='extract_xml', key='xml_root')
    
    if not xml_string:
        logger.warning("No XML data to transform")
        return []
    
    try:
        root = ET.fromstring(xml_string)
        transformed_data = []
        
        # Extract daily values if present
        daily_values = {}
        daily_values_elem = root.find('daily-values')
        if daily_values_elem is not None:
            for child in daily_values_elem:
                tag = child.tag.replace('-', '_')
                units = child.get('units', '')
                value = child.text.strip() if child.text else None
                daily_values[tag] = {
                    'value': value,
                    'units': units
                }
        
        # Extract food items
        foods = root.findall('food')
        
        for food in foods:
            try:
                # Extract basic food information
                name = food.find('name')
                name_text = name.text.strip() if name is not None and name.text else None
                
                mfr = food.find('mfr')
                mfr_text = mfr.text.strip() if mfr is not None and mfr.text else None
                
                serving = food.find('serving')
                serving_value = serving.text.strip() if serving is not None and serving.text else None
                serving_units = serving.get('units', '') if serving is not None else None
                
                # Extract calories
                calories = food.find('calories')
                calories_total = calories.get('total') if calories is not None else None
                calories_fat = calories.get('fat') if calories is not None else None
                
                # Extract nutritional values
                total_fat = food.find('total-fat')
                total_fat_value = total_fat.text.strip() if total_fat is not None and total_fat.text else None
                
                saturated_fat = food.find('saturated-fat')
                saturated_fat_value = saturated_fat.text.strip() if saturated_fat is not None and saturated_fat.text else None
                
                cholesterol = food.find('cholesterol')
                cholesterol_value = cholesterol.text.strip() if cholesterol is not None and cholesterol.text else None
                
                sodium = food.find('sodium')
                sodium_value = sodium.text.strip() if sodium is not None and sodium.text else None
                
                carb = food.find('carb')
                carb_value = carb.text.strip() if carb is not None and carb.text else None
                
                fiber = food.find('fiber')
                fiber_value = fiber.text.strip() if fiber is not None and fiber.text else None
                
                protein = food.find('protein')
                protein_value = protein.text.strip() if protein is not None and protein.text else None
                
                # Extract vitamins
                vitamins = food.find('vitamins')
                vitamin_a = None
                vitamin_c = None
                if vitamins is not None:
                    vit_a = vitamins.find('a')
                    vitamin_a = vit_a.text.strip() if vit_a is not None and vit_a.text else None
                    vit_c = vitamins.find('c')
                    vitamin_c = vit_c.text.strip() if vit_c is not None and vit_c.text else None
                
                # Extract minerals
                minerals = food.find('minerals')
                calcium = None
                iron = None
                if minerals is not None:
                    ca = minerals.find('ca')
                    calcium = ca.text.strip() if ca is not None and ca.text else None
                    fe = minerals.find('fe')
                    iron = fe.text.strip() if fe is not None and fe.text else None
                
                # Validate required fields
                if not name_text:
                    logger.warning(f"Skipping food item with missing name: {food}")
                    continue
                
                transformed_food = {
                    'name': name_text,
                    'manufacturer': mfr_text,
                    'serving_value': serving_value,
                    'serving_units': serving_units,
                    'calories_total': float(calories_total) if calories_total else None,
                    'calories_fat': float(calories_fat) if calories_fat else None,
                    'total_fat': float(total_fat_value) if total_fat_value else None,
                    'saturated_fat': float(saturated_fat_value) if saturated_fat_value else None,
                    'cholesterol': float(cholesterol_value) if cholesterol_value else None,
                    'sodium': float(sodium_value) if sodium_value else None,
                    'carb': float(carb_value) if carb_value else None,
                    'fiber': float(fiber_value) if fiber_value else None,
                    'protein': float(protein_value) if protein_value else None,
                    'vitamin_a': float(vitamin_a) if vitamin_a else None,
                    'vitamin_c': float(vitamin_c) if vitamin_c else None,
                    'calcium': float(calcium) if calcium else None,
                    'iron': float(iron) if iron else None,
                }
                
                transformed_data.append(transformed_food)
                
            except (ValueError, TypeError, AttributeError) as e:
                logger.warning(f"Failed to transform food item: {e}")
                continue
        
        logger.info(f"Transformed {len(transformed_data)} food items")
        
        # Push to XCom for downstream tasks
        context['ti'].xcom_push(key='transformed_data', value=transformed_data)
        
        return transformed_data
        
    except ET.ParseError as e:
        logger.error(f"Failed to parse XML: {e}")
        raise
    except Exception as e:
        logger.error(f"Transformation error: {e}")
        raise


def load_xml_data(**context) -> Dict[str, Any]:
    """
    Load transformed XML data into PostgreSQL using SQLAlchemy 2.0
    """
    logger.info("Loading XML data to PostgreSQL")
    
    # Pull transformed data from XCom
    transformed_data = context['ti'].xcom_pull(task_ids='transform_xml', key='transformed_data')
    
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
                        # Check if food already exists (by name)
                        check_query = text("""
                            SELECT name FROM nutrition_foods
                            WHERE name = :name
                        """)
                        result = conn.execute(
                            check_query,
                            {'name': row_dict['name']}
                        )
                        existing = result.fetchone()
                        
                        if existing:
                            # Update existing record
                            update_query = text("""
                                UPDATE nutrition_foods
                                SET manufacturer = :manufacturer,
                                    serving_value = :serving_value,
                                    serving_units = :serving_units,
                                    calories_total = :calories_total,
                                    calories_fat = :calories_fat,
                                    total_fat = :total_fat,
                                    saturated_fat = :saturated_fat,
                                    cholesterol = :cholesterol,
                                    sodium = :sodium,
                                    carb = :carb,
                                    fiber = :fiber,
                                    protein = :protein,
                                    vitamin_a = :vitamin_a,
                                    vitamin_c = :vitamin_c,
                                    calcium = :calcium,
                                    iron = :iron
                                WHERE name = :name
                            """)
                            conn.execute(update_query, row_dict)
                            skipped_count += 1
                        else:
                            # Insert new record
                            insert_query = text("""
                                INSERT INTO nutrition_foods
                                (name, manufacturer, serving_value, serving_units,
                                 calories_total, calories_fat, total_fat, saturated_fat,
                                 cholesterol, sodium, carb, fiber, protein,
                                 vitamin_a, vitamin_c, calcium, iron)
                                VALUES
                                (:name, :manufacturer, :serving_value, :serving_units,
                                 :calories_total, :calories_fat, :total_fat, :saturated_fat,
                                 :cholesterol, :sodium, :carb, :fiber, :protein,
                                 :vitamin_a, :vitamin_c, :calcium, :iron)
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
                'dag_id': 'xml_to_sql_pipeline',
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
                    'dag_id': 'xml_to_sql_pipeline',
                    'execution_date': datetime.now(),
                    'status': 'FAILED',
                    'error_message': str(e)
                })
        except Exception as log_error:
            logger.error(f"Failed to log error: {log_error}")
        
        raise


def validate_xml_data(**context) -> None:
    """
    Validate the loaded XML data using SQLAlchemy 2.0
    """
    logger.info("Validating loaded XML data")
    
    try:
        engine = create_engine(POSTGRES_CONNECTION, echo=False)
        
        with engine.connect() as conn:
            # Check total records
            count_query = text("SELECT COUNT(*) as total FROM nutrition_foods")
            result = conn.execute(count_query)
            total_records = result.scalar() or 0
            
            # Check for duplicates
            duplicate_query = text("""
                SELECT name, COUNT(*) 
                FROM nutrition_foods 
                GROUP BY name 
                HAVING COUNT(*) > 1
            """)
            duplicates_result = conn.execute(duplicate_query)
            duplicates = duplicates_result.fetchall()
            
            # Check data quality
            quality_query = text("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN name IS NULL OR name = '' THEN 1 ELSE 0 END) as missing_names,
                    SUM(CASE WHEN calories_total IS NULL THEN 1 ELSE 0 END) as missing_calories,
                    SUM(CASE WHEN calories_total IS NOT NULL AND calories_total < 0 THEN 1 ELSE 0 END) as invalid_calories
                FROM nutrition_foods
            """)
            quality_result = conn.execute(quality_query).fetchone()
            
            # Get manufacturer distribution
            manufacturer_query = text("""
                SELECT manufacturer, COUNT(*) as count
                FROM nutrition_foods
                WHERE manufacturer IS NOT NULL
                GROUP BY manufacturer
                ORDER BY count DESC
            """)
            manufacturer_result = conn.execute(manufacturer_query)
            manufacturer_dist = manufacturer_result.fetchall()
            
        logger.info(f"Validation results:")
        logger.info(f"  Total records: {total_records}")
        logger.info(f"  Duplicate food names: {len(duplicates)}")
        
        if quality_result:
            logger.info(f"  Missing names: {quality_result[1]}")
            logger.info(f"  Missing calories: {quality_result[2]}")
            logger.info(f"  Invalid calories: {quality_result[3]}")
        
        logger.info(f"  Manufacturer distribution:")
        for mfr_row in manufacturer_dist:
            logger.info(f"    {mfr_row[0]}: {mfr_row[1]}")
        
        if len(duplicates) > 0:
            logger.warning(f"Found {len(duplicates)} duplicate food names")
            
    except SQLAlchemyError as e:
        logger.error(f"Validation error: {e}")
        raise


# Define DAG tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

extract_xml_task = PythonOperator(
    task_id='extract_xml',
    python_callable=extract_xml_data,
    provide_context=True,
    dag=dag,
)

transform_xml_task = PythonOperator(
    task_id='transform_xml',
    python_callable=transform_xml_data,
    provide_context=True,
    dag=dag,
)

load_xml_task = PythonOperator(
    task_id='load_xml',
    python_callable=load_xml_data,
    provide_context=True,
    dag=dag,
)

validate_xml_task = PythonOperator(
    task_id='validate_xml',
    python_callable=validate_xml_data,
    provide_context=True,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start_task >> extract_xml_task >> transform_xml_task >> load_xml_task >> validate_xml_task >> end_task
