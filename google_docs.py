import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

APP_DIR = os.path.dirname(os.path.realpath(__file__))
CONGIF_DIR = os.path.join(APP_DIR, 'configuration_files')

json_key = json.load(open(os.path.join(CONGIF_DIR, 'google_secret.json')))
scope = ['https://spreadsheets.google.com/feeds']

credentials = ServiceAccountCredentials.from_json_keyfile_dict(json_key, scope)

GoogleDoc = gspread.authorize(credentials)