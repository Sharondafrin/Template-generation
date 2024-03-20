import logging
import os
import time
import pymysql
import json
import shutil
import warnings
# from datetime import datetime
from imagehash import hex_to_hash
import pandas as pd
from flask_cors import CORS
from flask import Flask, request, jsonify
# from config.config import config
from config.expression import languages_alpha3to2
from src.utils import create_table, insert_data, get_vendor, service_manager_loader
from src. template_generation import TemplateGeneration

warnings.filterwarnings(action='ignore')
service_manager = service_manager_loader()

# Archive Log Files
if os.path.exists(service_manager['PATH']['LOGGER']):

    time_now = time.time()
    if time_now - os.path.getctime(service_manager['PATH']['LOGGER']) > 6*30*24*3600:
        archive_dir = os.path.join(os.path.dirname(service_manager['PATH']['LOGGER']), 'archive')
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)
        shutil.move(service_manager['PATH']['LOGGER'], os.path.join(archive_dir, 'template-generation-' + str(int(time_now))))

logging.basicConfig(filename=service_manager['PATH']['LOGGER'],
                    level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
CORS(app)
logging.info('Service Restarted Successfully!\n')


@app.route('/', methods=["GET"])
def check():
    return "Service is Up and Running. Please Use /generate for Template Generation"


@app.route('/generate', methods=["POST"])
def generate_template():

    if request.method == 'POST':

        arguments = request.json

        logging.info('arguments: {}'.format(arguments.keys()))
        document_id = arguments.get('documentId')
        customer_cvr = str(arguments.get('customerVat'))
        country_code = arguments.get('countryCode')
        language = arguments.get('language')
        labels = arguments.get('labels')
        images = arguments.get('images')
        user_type = arguments.get('userType')

        response = {
            'status': 'OK'
        }
        _base_path = os.path.dirname(__file__)
        dir_name = document_id + '_' + str(int(time.time()))
        save_dir = os.path.join(_base_path, r'../uploads', dir_name)
        os.makedirs(save_dir)
        required_columns = ['imageId', 'keyBoundingBox', 'valueBoundingBox', 'croppedValueBoundingBox', 'labelText', 'value', 'key', 'score']

        with open(os.path.join(save_dir, document_id + '.json'), "w") as input_file:
            json.dump(arguments, input_file, indent=2)

        try:
            if labels:
                logging.info('\nReceived info : %s %s %s %s %s' % (
                    document_id, customer_cvr, country_code, language, user_type))
                template_obj = TemplateGeneration()

                logging.info('Extracted Information {}'.format(
                    [(label['key'], label['refinedValue']) for label in labels]))

                language = language.lower() if language.lower() in languages_alpha3to2.keys() else 'eng'

                output_df = template_obj.get_templates(images=images, labels=labels,
                                                       languages=languages_alpha3to2[language])

                vendor = [label['refinedValue'] for label in labels if label['key'] == 'vendorVat']
                temp_df = output_df[['imageId', 'hash', 'page']]
                temp_df.drop_duplicates('imageId', inplace=True, ignore_index=True)
                temp_df.set_index('imageId', inplace=True)
                output_df = output_df[required_columns]

                if not output_df.empty:
                    connection = pymysql.connect(
                        host=service_manager['DATABASE']['HOST'], port=int(service_manager['DATABASE']['PORT']),
                        user=service_manager['DATABASE']['USER'], password=service_manager['DATABASE']['PASSWORD'],
                        database=service_manager['DATABASE']['NAME'], connect_timeout=60)

                    cursor = connection.cursor()
                    vendor_cvr = get_vendor(vendor)
                    logging.info(f'vendor_cvr:{vendor_cvr}')
                    customer_cvr = 'GLOBAL' if user_type == 'HI' and vendor_cvr else customer_cvr

                    create_table(cursor, customer_cvr)
                    cursor.execute(f"""SELECT `index`, hash, epoch FROM `{customer_cvr}`""")
                    table_rows = cursor.fetchall()
                    column_names = ['index', 'hash', 'epoch']
                    table_rows = [row for row in table_rows]
                    table_df = pd.DataFrame(table_rows, columns=column_names)
                    table_df['epoch'] = table_df['epoch'].fillna(0).astype(int)
                    logging.info(f'Data Retrieval Successful for these columns!: {column_names}')

                    for image in output_df['imageId'].unique():
                        hash_id = temp_df.loc[image, 'hash']
                        hash_id = json.dumps(str(hash_id))

                        df = output_df[output_df['imageId'] == image]
                        # df['score'] = df['score'].apply(lambda x: int(x) if x in df['score'] else 999)
                        label = df.to_json(orient='records')

                        epoch = int(time.time())
                        page = temp_df.loc[image, 'page']

                        logging.info(f'epoch:{epoch}')

                        hamming_dist = [hex_to_hash(eval(table_hash)) - hex_to_hash(eval(hash_id)) for table_hash in
                                        table_df['hash'].tolist()]

                        table_df['hamming_distance'] = hamming_dist
                        sorted_hdist = table_df[table_df['hamming_distance'] == 0].sort_values(
                            by='epoch', ascending=False)
                        index_to_remove = sorted_hdist['index'].tolist()

                        logging.info(f'index to remove: {index_to_remove}')

                        for idx in index_to_remove:
                            cursor.execute(f"""DELETE FROM `{customer_cvr}` WHERE `index`={idx};""")

                        data = (
                                image, document_id, country_code, language, vendor_cvr, hash_id,
                                label, user_type, page, epoch
                                )

                        logging.info(f'Data To Be Inserted:{(image, document_id, country_code, language, vendor_cvr, hash_id, user_type, page, epoch)}')

                        insert_data(connection, cursor, customer_cvr, data)

                    cursor.close()
                    connection.close()

        except Exception as error:
            logging.info(f'Template Generation Failed! {error}')
            response = {
                'status': f'Failed: {error}',
            }

        response = jsonify(response)
        response.headers.add('Access-Control-Allow-Origin', '*')

        return response


@app.route('/reset', methods=["POST"])
def reset_templates():
    if request.method == "POST":

        arguments = request.json
        customer_cvr = arguments['customerVat'].upper()
        vendor_cvr = arguments['vendorVat'].upper()
        user_type = arguments['userType'] if 'userType' in arguments.keys() else 'HI'
        logging.info(f'arguments: {customer_cvr, vendor_cvr, user_type}')
        response = {
            'status': 'OK',
            'message': 'No Records Found'
        }

        try:
            if user_type == 'CUSTOMER':
                connection = pymysql.connect(
                            host=service_manager['DATABASE']['HOST'], port=int(service_manager['DATABASE']['PORT']),
                            user=service_manager['DATABASE']['USER'], password=service_manager['DATABASE']['PASSWORD'],
                            database=service_manager['DATABASE']['NAME'], connect_timeout=60)

                cursor = connection.cursor()
                cursor.execute(f"""SELECT `index`, vendorVat FROM `{customer_cvr}` WHERE vendorVat='{vendor_cvr}'""")
                table_rows = cursor.fetchall()
                column_names = ['index', 'vendor_vat']
                table_rows = [row for row in table_rows]
                table_df = pd.DataFrame(table_rows, columns=column_names)
                index_to_remove = table_df['index'].tolist()
                logging.info(f'index_to_remove: {index_to_remove}')

                if index_to_remove:
                    for idx in index_to_remove:
                        cursor.execute(f"""DELETE FROM `{customer_cvr}` WHERE `index`={idx};""")
                    response['message'] = 'Reset Completed'
                    logging.info('Templates removed successfully')

                connection.commit()
                cursor.close()
                connection.close()

        except Exception as error:
            logging.info(f'Template reset failed!: {error}')
            response = {
                'status': 'Failed',
                'message': error}

        return jsonify(response)


if __name__ == "__main__":
    app.run(port=3036, debug=True)
