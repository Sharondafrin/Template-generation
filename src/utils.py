# import requests
import logging
import os.path
import re
from config.config import config
import configparser

def create_database(cursor):

    create_database_query = "CREATE DATABASE Template_Generation_DB"
    cursor.excute(create_database_query)


def get_vendor(vendor_list):

    vendor = [re.sub(r'[ .~_:;,\-]+', '', _vendor) for _vendor in vendor_list if _vendor]
    vendor = vendor[0] if vendor else ''

    return vendor


def create_table(cursor, table_name):

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `index` INT AUTO_INCREMENT PRIMARY KEY,
            imageId TEXT,
            documentId TEXT,
            countryCode TEXT,
            language TEXT,
            vendorVat TEXT,
            hash TEXT,
            labels TEXT,
            userType TEXT,         
            page INT,          
            epoch INT       
        );
        """
    cursor.execute(create_table_query)
    logging.info(f"Table '{table_name}' created successfully.")


def insert_data(connection, cursor, table_name, record):

    try:
        # image, document_id, country_code, language, vendor_cvr, label = record
        # logging.info('Record to be inserted {}'.format(record))

        insert_query = f"""INSERT INTO `{table_name}`
                           (imageId, documentId, countryCode, language, vendorVat, hash, labels, userType, page, epoch) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        cursor.execute(insert_query, record)
        connection.commit()

        logging.info("Record Inserted Successfully")

    except Exception as error:

        logging.error(f"Error Occurred During Data Insertion: {error}")
        raise


def get_unique(raw_list):
    unique_values = []
    for item in raw_list:
        if item not in unique_values:
            unique_values.append(item)
    return unique_values


def flatten_list(item):
    container = []
    if isinstance(item, (tuple, list)):
        for entity in item:
            if not isinstance(entity, (tuple, list)):
                container.append(entity)
            else:
                container += flatten_list(entity)
    else:
        container = [item]

    return container


def service_manager_loader():
    service_manager = configparser.ConfigParser()
    if os.path.exists(config['SERVICE_MANAGER']['GLOBAL']):
        service_manager.read(config['SERVICE_MANAGER']['GLOBAL'])
    else:
        service_manager.read(config['SERVICE_MANAGER']['LOCAL'])

    return service_manager

