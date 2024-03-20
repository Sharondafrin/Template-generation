import os
import json
import logging
import pandas as pd
# from glob import glob
import base64
import requests
from io import BytesIO
from PIL import Image
import imagehash
import warnings
from config.expression import languages_alpha3to2
warnings.filterwarnings(action='ignore')

# from src.utils import create_request, get_content_type
# from config.expression import languages_alpha3to2


class TemplateGeneration:

    def __init__(
            self, tolerance=0.1, confidence_score=0.7, img_extension='.jpg',  max_pages=4,
    ):
        self.retina_url = 'http://server-name:port/endpoint-name'
        self.tolerance = tolerance
        self.confidence_score = confidence_score
        self.img_extension = img_extension
        self.max_pages = max_pages

    def get_bboxes(self, images, languages):

        bbox_df = []
        retina_input = {'images': [image for image in images if not image['boundingBox']],
                        'languages': languages}
        # Add a condition to check retina input is empty

        payload = json.dumps(retina_input)

        headers = {'Content-Type': 'application/json'}
        retina_output = requests.post(url=self.retina_url, data=payload, headers=headers)
        retina_bboxes = json.loads(retina_output.content)
        print('retina_bboxes', retina_bboxes.keys())

        for image_block in images:
            image_name = image_block['imageName']
            # df = pd.DataFrame(json.loads(image_block['boundingBox']))
            df = pd.DataFrame(json.loads(retina_bboxes[image_name])) if not image_block['boundingBox'] else \
                pd.DataFrame(json.loads(image_block['boundingBox']))

            image_bytes = base64.b64decode(image_block['encodedImage'])
            image = Image.open(BytesIO(image_bytes))
            image_phash = imagehash.phash(image)

            df['hash'] = image_phash

            df['page'] = int(image_block['pageNumber'])
            df[['xmin', 'ymin', 'xmax', 'ymax']] = df['coordinates'].apply(
                lambda x: pd.Series(x[:4], index=['xmin', 'ymin', 'xmax', 'ymax']))

            df['width_tol'] = df[['xmax', 'xmin']].apply(lambda x: (x['xmax'] - x['xmin']) * self.tolerance, axis=1)
            df['height_tol'] = df[['ymax', 'ymin']].apply(lambda x: (x['ymax'] - x['ymin']) * self.tolerance, axis=1)
            df['imageId'] = image_block['imageName'].replace('.jpg', '')

            df['xmin_ext'] = df[['xmin', 'width_tol']].apply(lambda x: x['xmin'] - x['width_tol'], axis=1)
            df['xmax_ext'] = df[['xmax', 'width_tol']].apply(lambda x: x['xmax'] + x['width_tol'], axis=1)
            df['ymin_ext'] = df[['ymin', 'height_tol']].apply(lambda x: x['ymin'] - x['height_tol'], axis=1)
            df['ymax_ext'] = df[['ymax', 'height_tol']].apply(lambda x: x['ymax'] + x['height_tol'], axis=1)
            bbox_df.append(df)

        bbox_df = pd.concat(bbox_df)
        # print('bbox_df.shape', bbox_df.shape)
        bbox_df.reset_index(drop=True, inplace=True)

        logging.info('Preprocessing Retina Result Completed!')
        return bbox_df

    @staticmethod
    def get_output(labels):

        label_df = pd.DataFrame(labels)
        label_df.fillna('', inplace=True)
        label_df['keyBoundingBox'] = label_df['keyBoundingBox'].apply(
            lambda x: x[:-1] if isinstance(x, list) and x else [])
        label_df['croppedValueBoundingBox'] = label_df['valueBoundingBox'].apply(
            lambda x: x[:-1] if isinstance(x, list) and x else [])
        label_df = label_df[label_df['valueBoundingBox'].apply(lambda x: True if x else False)]

        label_df[['xmin', 'ymin', 'xmax', 'ymax', 'page']] = label_df['valueBoundingBox'].apply(
            lambda x: pd.Series(x, index=['xmin', 'ymin', 'xmax', 'ymax', 'page']))

        label_df['page'] = label_df['page'].apply(lambda x: int(x))
        label_df['score'] = label_df['score'].apply(lambda x: x if x else 999)
        label_df['box_area'] = label_df[['xmin', 'ymin', 'xmax', 'ymax']].apply(
            lambda x: (x['xmax']-x['xmin'])*(x['ymax'] - x['ymin']), axis=1)

        label_df.reset_index(inplace=True, drop=True)
        label_df['index'] = label_df.index

        return label_df

    def calculate_overlap(self, retina_box, page_df, data_dir=None):

        xmin, ymin, xmax, ymax = retina_box
        page_df['x_overlap'] = page_df[['xmin', 'xmax']].apply(
            lambda x: max(0, min(x['xmax'], xmax) - max(x['xmin'], xmin)), axis=1)
        page_df['y_overlap'] = page_df[['ymin', 'ymax']].apply(
            lambda x: max(0, min(x['ymax'], ymax) - max(x['ymin'], ymin)), axis=1)

        page_df['overlap_score'] = page_df[['x_overlap', 'y_overlap', 'box_area']].apply(
            lambda x: (x['x_overlap'] * x['y_overlap']) / x['box_area'], axis=1)

        if data_dir:
            page_df.to_csv(os.path.join(data_dir, 'page_df.csv'))
        return list(page_df[page_df['overlap_score'] >= self.confidence_score]['index'])

    def get_templates(self, images, labels, languages, data_dir=None):

        label_df = self.get_output(labels)
        output_df = []

        if not label_df.empty:

            valid_pages = list(label_df['page'].unique())

            images = [image for image in images if (int(image['pageNumber']) in valid_pages)
                      & (int(image['pageNumber']) < self.max_pages)]
            print('image information', [image['imageName'] for image in images])

            bbox_df = self.get_bboxes(images, languages)

            for page in bbox_df['page'].unique():

                box_df = bbox_df[bbox_df['page'] == page]
                page_df = label_df[label_df['page'] == int(page)]
                # if data_dir:
                #     page_df.to_csv(os.path.join(data_dir, 'page_{page}.csv'))
                #     box_df.to_csv(os.path.join(data_dir, 'box_{page}.csv'))
                # box_df.rename(columns={'score':'easyocr_score'}, inplace = True)
                box_df['coordinate_ext'] = box_df[['xmin_ext', 'ymin_ext', 'xmax_ext', 'ymax_ext']].apply(
                    lambda x: list(x), axis=1)
                box_df['matched_index'] = box_df['coordinate_ext'].apply(
                    lambda x: self.calculate_overlap(x, page_df) if x else [])

                output_df.append(box_df)

            output_df = pd.concat(output_df)

            output_df = output_df.explode('matched_index')

            if not output_df.empty:
                references = ('xmin', 'ymin', 'xmax', 'ymax')
                output_df['matched_index'] = output_df['matched_index'].apply(lambda x: x if x >= 0 else 999)

                output_df['keyBoundingBox'] = output_df['matched_index'].apply(
                    lambda x: label_df.loc[x, 'keyBoundingBox'] if x in label_df.index else [])
                output_df['croppedValueBoundingBox'] = output_df['matched_index'].apply(
                    lambda x: label_df.loc[x, 'croppedValueBoundingBox'] if x in label_df.index else [])
                output_df['labelText'] = output_df['matched_index'].apply(
                    lambda x: label_df.loc[x, 'labelText'] if x in label_df.index else '')
                output_df['value'] = output_df['matched_index'].apply(
                    lambda x: label_df.loc[x, 'refinedValue'] if x in label_df.index else '')
                output_df['key'] = output_df['matched_index'].apply(
                    lambda x: label_df.loc[x, 'key'] if x in label_df.index else 'other')

                output_df['valueBoundingBox'] = output_df[['xmin', 'ymin', 'xmax', 'ymax']].apply(
                    lambda x: {ref: x[ref] for ref in references}, axis=1)
                output_df['croppedValueBoundingBox'] = output_df['croppedValueBoundingBox'].apply(
                    lambda x: {ref: x[i] for i, ref in enumerate(references)} if x else {})
                output_df['keyBoundingBox'] = output_df['keyBoundingBox'].apply(
                    lambda x: {ref: x[i] for i, ref in enumerate(references)} if x else {})
                output_df['score'] = output_df['matched_index'].apply(
                    lambda x: label_df.loc[x, 'score'] if x in label_df.index else -1)

                # if data_dir:
                #     required_columns = ['imageId', 'keyBoundingBox', 'valueBoundingBox', 'labelText', 'value', 'key',
                #                         'score']
                #     label_df.to_csv(os.path.join(data_dir, 'label_df.csv'))
                #     bbox_df.to_csv(os.path.join(data_dir, 'bbox_df.csv'))
                #     output_df[required_columns].to_csv(os.path.join(data_dir, 'output_df.csv'))

            logging.info('Template Created successfully!')

        else:
            logging.info('No Valid Inputs from Portal')

        return output_df


if __name__ == '__main__':
    invoice_dir = r"D:\template-input\TG\REV1657019105956TG.json"

    file = open(invoice_dir, "rb")
    ocr_response = json.load(file)

    _language = ocr_response['language']
    _labels = ocr_response['labels']
    _images = ocr_response['images']
    _data_dir = os.path.dirname(invoice_dir)
    template_obj = TemplateGeneration()

    _language = _language.lower() if _language.lower() in languages_alpha3to2.keys() else 'eng'
    print('_language', _language)
    _output_df = template_obj.get_templates(images=_images, labels=_labels,
                                            languages=languages_alpha3to2[_language], data_dir=_data_dir)

    _output_df.to_csv(os.path.join(_data_dir, 'output.csv'), index=False)
