from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import xml.etree.ElementTree as ET
import pandas as pd
import os

def extract_json_data():
    try:
        with open('/opt/airflow/dags/pets.json', 'r') as f:
            data = json.load(f)
        
        pets_list = []
        for pet in data['pets']:
            flat_pet = {
                'name': pet.get('name', ''),
                'species': pet.get('species', ''),
                'favFoods': ', '.join(pet.get('favFoods', [])) if pet.get('favFoods') else '',
                'birthYear': pet.get('birthYear', ''),
                'photo': pet.get('photo', '')
            }
            pets_list.append(flat_pet)
        
        df = pd.DataFrame(pets_list)
        output_path = '/opt/airflow/output/pets_flat.csv'
        os.makedirs('/opt/airflow/output', exist_ok=True)
        df.to_csv(output_path, index=False)
        
        print("JSON flattened successfully")
        print(df)
        return output_path
    except Exception as e:
        print(f"JSON processing error: {e}")
        return None

def extract_xml_data():
    try:
        tree = ET.parse('/opt/airflow/dags/nutrition.xml')
        root = tree.getroot()
        
        foods_list = []
        for food in root.findall('food'):
            vitamins_elem = food.find('vitamins')
            minerals_elem = food.find('minerals')
            
            food_dict = {
                'name': food.find('name').text if food.find('name') is not None else '',
                'mfr': food.find('mfr').text if food.find('mfr') is not None else '',
                'serving_value': food.find('serving').text if food.find('serving') is not None else '',
                'serving_units': food.find('serving').get('units', '') if food.find('serving') is not None else '',
                'calories_total': food.find('calories').get('total', '') if food.find('calories') is not None else '',
                'calories_fat': food.find('calories').get('fat', '') if food.find('calories') is not None else '',
                'total_fat': food.find('total-fat').text if food.find('total-fat') is not None else '',
                'saturated_fat': food.find('saturated-fat').text if food.find('saturated-fat') is not None else '',
                'cholesterol': food.find('cholesterol').text if food.find('cholesterol') is not None else '',
                'sodium': food.find('sodium').text if food.find('sodium') is not None else '',
                'carb': food.find('carb').text if food.find('carb') is not None else '',
                'fiber': food.find('fiber').text if food.find('fiber') is not None else '',
                'protein': food.find('protein').text if food.find('protein') is not None else '',
                'vitamin_a': vitamins_elem.find('a').text if vitamins_elem is not None else '',
                'vitamin_c': vitamins_elem.find('c').text if vitamins_elem is not None else '',
                'calcium': minerals_elem.find('ca').text if minerals_elem is not None else '',
                'iron': minerals_elem.find('fe').text if minerals_elem is not None else ''
            }
            foods_list.append(food_dict)
        
        df = pd.DataFrame(foods_list)
        output_path = '/opt/airflow/output/foods_flat.csv'
        os.makedirs('/opt/airflow/output', exist_ok=True)
        df.to_csv(output_path, index=False)
        
        print("XML flattened successfully")
        print(df)
        return output_path
    except Exception as e:
        print(f"XML processing error: {e}")
        return None

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='etl_complex_formats_local',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:
    
    task_json = PythonOperator(
        task_id='process_json',
        python_callable=extract_json_data
    )
    
    task_xml = PythonOperator(
        task_id='process_xml',
        python_callable=extract_xml_data
    )
    
    task_json >> task_xml
