from oauth2client.service_account import ServiceAccountCredentials
from apiclient.http import MediaFileUpload
import googleapiclient.discovery
from google.cloud import storage
import requests
import pandas as pd
import datetime
import base64
import json


def cost_data_importer(event, context):

    # General request URL
    request_url = 'http://reports.tradedoubler.com/pan/mReport3Key.action'

    # Set a dynmic date for the data you wish to export (in this case yesterdays date)
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days = 1)
    
    # Decode and use values from sub/pub message
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

    # Define the parameters (The key needs to acquired from Tradedoubler, programID or IDs are unique to the customer)
    parameters = {
        'key' : '3bde99bd26c0852dcbd318619730325d',
        'reportName' : 'mMerchantOverviewReport',
        'programId' : message['programId'],
        'columns' : ['siteName', 'impNrOf', 'clickNrOf', 'totalCommission', 'programId'],
        'startDate' : yesterday.strftime("%d/%m/%y"),
        'endDate' : yesterday.strftime("%d/%m/%y"),
        'currencyId': message['currency'],
        'format': 'CSV'
    }

    # Optional - print the full request URL
    api_output = requests.get(request_url, params=parameters)
    print(api_output.url)

    """## Create dataframe for upload"""

    # Grab the contents of the response
    api_output = requests.get(request_url, params=parameters).content
    api_output = api_output.decode('utf-8')
    print(api_output)

    # Split the returned CSV values
    api_output_split = api_output.splitlines()
    api_output_split = [w.replace(u'\xa0', '') for w in api_output_split]
    print(api_output_split)

    api_output_list = []

    for i in range(len(api_output_split)):
      api_output_list.append(api_output_split[i].split(';'))

    # Create a dataframe from the list
    df_to_upload = pd.DataFrame.from_records(api_output_list)

    # Replace commas and add necessary coloumns for upload
    df_to_upload[3] = df_to_upload[3].str.replace(',','.')
    df_to_upload[4] = yesterday.strftime("%Y%m%d")
    df_to_upload[5] = 'OVERWRITE'
    df_to_upload[6] = message['source']
    df_to_upload[7] = 'affiliate'

    #Rename columns and trim headers and total rows
    df_to_upload.rename(
      columns={
          0 : 'ga:campaign',
          1 : 'ga:impressions',
          2 : 'ga:adClicks',
          3 : 'ga:adCost',
          4 : 'ga:date',
          5 : 'ga:importBehavior',
          6 : 'ga:source',
          7 : 'ga:medium',
      },
      inplace=True
    )
    df_to_upload = df_to_upload[2:-1]

    # Save dataframe as a .csv file
    df_to_upload.to_csv(path_or_buf='/tmp/test_upload.csv',index=False,encoding='utf-8')

    ## GA input variables
    ga_account_id = message['accountId']
    ga_property_id = message['propertyId']
    ga_data_set_id = message['datasetId']

    # The credential associated with the service account (this allows the script to act as the service account)
    client = storage.Client()
    bucket = client.get_bucket('data_uploader')
    blob = bucket.blob('precis-internal-gbg-d0803f11ffb7.json')

    blob.download_to_filename('/tmp/auth.json')

    # The Google Analytics scope required to perform the action
    scope = ['https://www.googleapis.com/auth/analytics']

    credentials = ServiceAccountCredentials.from_json_keyfile_name('/tmp/auth.json', scopes=scope)
    service = googleapiclient.discovery.build('analytics', 'v3', credentials=credentials)

    # Use the MediaFileUpload object to handle the .csv file during the API request
    media_to_upload = MediaFileUpload('/tmp/test_upload.csv',
                            mimetype='application/octet-stream',
                            resumable=False)

    # API request to upload dataframe to the specified Google Analytics data source
    service.management().uploads().uploadData(
          accountId=ga_account_id,
          webPropertyId=ga_property_id,
          customDataSourceId=ga_data_set_id,
          media_body=media_to_upload).execute()
