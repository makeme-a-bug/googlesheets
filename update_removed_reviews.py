from __future__ import print_function
from multiprocessing.connection import wait

import os.path
from re import I, S
from typing import List

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import gspread
from scrapingbee import ScrapingBeeClient
from concurrent.futures import ThreadPoolExecutor
import urllib.parse
import time
# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
scraping_bee_client = ScrapingBeeClient(api_key='YOK6YU7PX88YFFNQJKPUGSF6KA5N0FEVPR8GK04UAU6TX38IOXV5ZJBU56OBFSAH8ZMIRZJX6LFS59M6')



def get_creds():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

creds = get_creds()
client = gspread.authorize(creds)


def get_reviews_link() -> List[List[str]]:


    try:

        spread_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1GB4GaoaqQzQqDlWKjOMN8c0VPeW5GHAW5ZtDCNwWTu4/edit#gid=2064478844")
        sheet = spread_sheet.worksheet("Negative Review Service Dashboard")
        # Call the Sheets API
        values = sheet.col_values(13)

        if not values:
            print('No data found.')
            return

        reviews_link = list(filter(lambda x: x.startswith("https"),values))

        return reviews_link[1:]

    except Exception as e:
        print("couldn't work with master sheet")
        print(e)



def check_status(link):
    response = scraping_bee_client.get(link,params = { 
        'render_js': 'False',
        'json_response':"True"
    })

    try:
        status = response.headers.get('Spb-initial-status-code',None)
        return int(status)
    except:
        return None



def update_reviews_removed(reviews_sheet_link):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    print("working on " , reviews_sheet_link)
    if not reviews_sheet_link:
        return

    spread_sheet = client.open_by_url(reviews_sheet_link)
    sheet = spread_sheet.worksheet("All 1-2 Star Reviews")

    reviews_col = None
    removed_col = None
    i=1

    columns =sheet.row_values(1)

    for i , column in enumerate(columns):
        if  column == "Link to Review":
            reviews_col = i+1

        if  column == "Removed":
            removed_col = i+1
    
    if reviews_col is None or removed_col is None:
        print(f"failed to find columns in {reviews_sheet_link}") 
        return
    
    print("found all columns")
    all_reviews = sheet.col_values(reviews_col)
    all_removed = sheet.col_values(removed_col)


    for i in range(1,len(all_reviews)):
        if all_removed[i] == "FALSE":
            status = check_status(all_reviews[i])
            if status == 404:
                print("found one review removed",all_reviews[i])
                all_removed[i] = "TRUE"
    sheet.update(f'{chr(64 + removed_col) }:{chr(64 + removed_col)}', [[all_removed[0]]]+[ [False] if r == "FALSE" else [True] for r in all_removed[1:]])
    print('done',reviews_sheet_link)



def main():
    print("getting all the reviews sheet links")
    monitering_reviews_link = get_reviews_link()
    print(f"found {len(monitering_reviews_link)} links from the masters sheet.")

    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(update_reviews_removed,monitering_reviews_link)
    print("updates done")
    # update_reviews_removed(monitering_reviews_link[0])
if __name__ == '__main__':
    main()









