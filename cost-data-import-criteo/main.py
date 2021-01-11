from oauth2client.service_account import ServiceAccountCredentials
from apiclient.http import MediaFileUpload
import googleapiclient.discovery
from google.cloud import storage
import requests
import pandas as pd
import datetime
import base64
import json
import criteo_marketing as cm
from criteo_marketing import Configuration

def cost_data_importer(event, context):

    # Set a dynmic date for the data you wish to export (in this case yesterdays date)
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days = 1)
    
    # Decode and use values from sub/pub message
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

     #Set the credentials
    GRANT_TYPE = 'client_credentials'

    configuration = Configuration()
    configuration.username = message['username']
    configuration.password = message['password']

    client = cm.ApiClient(configuration)

    auth_api = cm.AuthenticationApi(client)
    auth_response = auth_api.o_auth2_token_post(client_id=client.configuration.username,
                                              client_secret=client.configuration.password,
                                              grant_type=GRANT_TYPE)
    token = auth_response.token_type + " " + auth_response.access_token

    stats_api = cm.StatisticsApi(client)
    
    # Define the parameters
    stats_query_message = cm.StatsQueryMessage(
    dimensions = ["CampaignId"],
    metrics = ["Displays", "Clicks", "AdvertiserCost"],
    start_date = yesterday,
    end_date = yesterday,
    currency = "SEK",
      format="Csv")

    """## Create dataframe for upload"""

    # Grab the contents of the response
    api_output = stats_api.get_stats(token, stats_query_message)

    # Split the returned CSV values
    api_output_split = api_output.splitlines()
    api_output_split = [w.replace('\ufeff', '') for w in api_output_split]

    api_output_list = []

    for i in range(len(api_output_split)):
      api_output_list.append(api_output_split[i].split(';'))

    #Create a dataframe from the list
    df_to_upload = pd.DataFrame.from_records(api_output_list)

    new_header = df_to_upload.iloc[0] #grab the first row for the header
    df_to_upload = df_to_upload[1:] #take the data less the header row
    df_to_upload.columns = new_header #set the header row as the df header

    #Rename columns to GA schema
    df_to_upload.rename(
      columns={
          'Impressions' : 'ga:impressions',
          'Clicks' : 'ga:adClicks', 
          'Cost' : 'ga:adCost',
          'Campaign Name' : 'ga:campaign'
      },
      inplace=True
    )
    #Round cost
    df_to_upload['ga:adCost'] = df_to_upload['ga:adCost'].astype(float)
    df_to_upload['ga:adCost'] = df_to_upload['ga:adCost'].round(decimals=0)

    yesterday = yesterday.strftime("%Y%m%d")

    #Add necessary coloumns for upload
    df_to_upload['ga:importBehavior'] = 'OVERWRITE'
    df_to_upload['ga:source'] = 'criteo'
    df_to_upload['ga:medium'] = 'banner'
    df_to_upload['ga:date'] = yesterday

    #Drop unnecessary columns
    df_to_upload = df_to_upload.drop(columns=["Campaign ID", "Advertiser Name", "Currency"], axis=1)

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