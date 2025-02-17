import os
import sys
from google.cloud import storage, bigquery, language, vision, translate_v2

if ('GOOGLE_APPLICATION_CREDENTIALS' in os.environ):
    if (not os.path.exists(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])):
        print ("The GOOGLE_APPLICATION_CREDENTIALS file does not exist.\n")
        exit()
else:
    print ("The GOOGLE_APPLICATION_CREDENTIALS environment variable is not defined.\n")
    exit()

if len(sys.argv)<3:
    print('You must provide parameters for the Google Cloud project ID and Storage bucket')
    print ('python3 '+sys.argv[0]+ '[PROJECT_NAME] [BUCKET_NAME]')
    exit()

project_name = sys.argv[1]
bucket_name = sys.argv[2]

# Set up our GCS, BigQuery, and Natural Language clients
storage_client = storage.Client()
bq_client = bigquery.Client(project=project_name)
nl_client = language.LanguageServiceClient()

# Set up client objects for the vision and translate_v2 API Libraries
vision_client = vision.ImageAnnotatorClient()
translate_client = translate_v2.Client()

# Setup the BigQuery dataset and table objects
dataset_ref = bq_client.dataset('image_classification_dataset')
dataset = bigquery.Dataset(dataset_ref)
table_ref = dataset.table('image_text_detail')
table = bq_client.get_table(table_ref)

# Create an array to store results data to be inserted into the BigQuery table
rows_for_bq = []

# Get a list of the files in the Cloud Storage Bucket
files = storage_client.bucket(bucket_name).list_blobs()
bucket = storage_client.bucket(bucket_name)

print('Processing image files from GCS. This will take a few minutes..')

# Process files from Cloud Storage and save the result to send to BigQuery
for file in files:
    if file.name.endswith('jpg') or file.name.endswith('png'):
        file_content = file.download_as_string()

        # Create Vision API image object
        image = vision.Image(content=file_content)

        # Detect text in the image using Vision API
        response = vision_client.document_text_detection(image=image)

        # Save the text content found by the vision API into a variable called text_data
        text_data = response.text_annotations[0].description

        # Save the text detection response data in <filename>.txt to cloud storage
        file_name = file.name.split('.')[0] + '.txt'
        blob = bucket.blob(file_name)
        blob.upload_from_string(text_data, content_type='text/plain')

        # Extract description and locale from the response
        desc = response.text_annotations[0].description
        locale = response.text_annotations[0].locale

        # Translate text if necessary
        if locale != '':
            translation = translate_client.translate(desc, target_language='ja')  # Change 'en' to desired target language
            translated_text = translation['translatedText']
        else:
            translated_text = desc

        print(translated_text)

        # If there is response data, append the information to rows_for_bq
        if len(response.text_annotations) > 0:
            rows_for_bq.append((desc, locale, translated_text, file.name))

print('Writing Vision API image data to BigQuery...')

# Write original text, locale, and translated text to BigQuery
errors = bq_client.insert_rows(table, rows_for_bq)
if errors:
    print(f"Errors occurred while inserting data into BigQuery: {errors}")
else:
    print("Data successfully inserted into BigQuery.")
