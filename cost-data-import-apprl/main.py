from oauth2client.service_account import ServiceAccountCredentials
from apiclient.http import MediaFileUpload
import googleapiclient.discovery
from google.cloud import storage
import requests
import pandas as pd
import datetime
import base64
import json

def cost_data_import(event, context):

    message_data = base64.b64decode(event['data']).decode('utf-8')
    
    attributes = event['attributes']
    
    print(message_data)
    print(attributes)
    
    pubsub_message = json.loads(json.loads(message_data))

    df_to_upload = pd.DataFrame(pubsub_message)

    df_to_upload

    #Add necessary coloumns for upload
    df_to_upload['ga:source'] = 'apprl'
    df_to_upload['ga:medium'] = 'affiliate'

    #Rename columns to GA schema
    df_to_upload.rename(
        columns={
            'clickDate' : 'ga:date',
            'clickNrOf' : 'ga:adClicks', 
            'cost' : 'ga:adCost',
            'campaignName' : 'ga:campaign'
        },
        inplace=True
    )
    
    df_to_upload['ga:date'] = df_to_upload['ga:date'].replace(regex=r'-', value='')
    
    df_to_upload

    # Save dataframe as a .csv file
    df_to_upload.to_csv(path_or_buf='/tmp/apprl_upload.csv',index=False,encoding='utf-8')
    
    #Define datasetId
    country = attributes['country'] 
    #TODO change dataset Id
    if country == "SE":
        datasetId = "xxx"
    elif country == "DK":
        datasetId = "xxx"
    elif country == "FI":
        datasetId = "xxx"
    elif country == "NO":
        datasetId = "xxx"
    else :
        datasetId = "s-Aw7A7GRE2vqL0jZ6dmXQ"

    ## GA input variables
    ga_account_id = '12345' #TODO add client account Id
    ga_property_id = 'UA-XXXXX-X' #TODO add client property Id
    ga_data_set_id = datasetId

    # The credential associated with the service account (this allows the script to act as the service account)
    client = storage.Client()
    bucket = client.get_bucket('data_uploader')
    blob = bucket.blob('') #TODO insert credentials bucket

    blob.download_to_filename('/tmp/auth.json')

    # The Google Analytics scope required to perform the action
    scope = ['https://www.googleapis.com/auth/analytics']

    credentials = ServiceAccountCredentials.from_json_keyfile_name('/tmp/auth.json', scopes=scope)
    service = googleapiclient.discovery.build('analytics', 'v3', credentials=credentials)

    # Use the MediaFileUpload object to handle the .csv file during the API request
    media_to_upload = MediaFileUpload('/tmp/apprl_upload.csv',
                                      mimetype='application/octet-stream',
                                      resumable=False)

    # API request to upload dataframe to the specified Google Analytics data source
    service.management().uploads().uploadData(
        accountId=ga_account_id,
        webPropertyId=ga_property_id,
        customDataSourceId=ga_data_set_id,
        media_body=media_to_upload).execute()
