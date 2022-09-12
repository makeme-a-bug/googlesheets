from __future__ import print_function

import os.path
from typing import List,Union

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

import gspread
from scrapingbee import ScrapingBeeClient
from concurrent.futures import ThreadPoolExecutor
import requests

from rich.console import Console

import pandas as pd
# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
scraping_bee_client = ScrapingBeeClient(api_key='YOK6YU7PX88YFFNQJKPUGSF6KA5N0FEVPR8GK04UAU6TX38IOXV5ZJBU56OBFSAH8ZMIRZJX6LFS59M6')#YOK6YU7PX88YFFNQJKPUGSF6KA5N0FEVPR8GK04UAU6TX38IOXV5ZJBU56OBFSAH8ZMIRZJX6LFS59M6
console = Console()


def get_creds():
    """
    Gets credentials from google using google Oauth.
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

    """
    Get all the reviews link sheets from master google spread sheet
    """

    try:

        spread_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1GB4GaoaqQzQqDlWKjOMN8c0VPeW5GHAW5ZtDCNwWTu4/edit#gid=2064478844")
        sheet = spread_sheet.worksheet("Negative Review Service Dashboard")
        # Call the Sheets API
        values = sheet.col_values(13)

        if not values:
            print('No data found.')
            return

        reviews_link = list(filter(lambda x: x.startswith("https"),values))

        return reviews_link

    except Exception as e:
        print("couldn't work with master sheet")
        print(e)



def check_status(link:str)-> Union[int,None]:
    """
    Checks the status of review link.
    """
    try:
        response = scraping_bee_client.get(link,params = { 
            'render_js': 'False',
            'json_response':"True"
        })

        try:
            status = response.headers.get('Spb-initial-status-code',None)
            return int(status)
        except:
            return None

    except requests.exceptions.ConnectTimeout as e:
        print("timed out connecting to scrapingbee")

    except requests.exceptions.ConnectionError as e:
        print("make sure your connected to internet")

    except requests.exceptions.RequestException as e:
        print("Fatal error : ",e)

    except Exception as e:
        print(e)
        print(link)
    
    return None



def generate_report(link:str,spread_sheet , report:List) -> None:
    try:
        console.log(f"generating report for: {link}",style="purple")
        report_sheet = spread_sheet.worksheet("Input - Removed Review URLs")
        report_sheet.clear()
        if len(report) > 0:
            report_sheet.append_row([url['Review Link'] for url in report ])

        console.log(f"report generated for : {link}",style="blue")
    except:
        print(f"Input - Removed Review URLs sheet could not be opened for {link}")

    return


def update_reviews_removed(reviews_sheet_link:str) -> List:
    """
    gets the reviews link sheet. goes through each link that is not removed and checks its status and
    updates it in the google spread sheet
    """
    console.log(f"working on: {reviews_sheet_link}",style="purple")
    if not reviews_sheet_link:
        return []

    spread_sheet = client.open_by_url(reviews_sheet_link)
    sheet = spread_sheet.worksheet("All 1-2 Star Reviews")

    reviews_col = None
    removed_col = None

    table = pd.DataFrame(sheet.get_all_records())


    for i , column in enumerate(table.columns):
        if  column == "Link to Review":
            reviews_col = i+1

        if  column == "Removed":
            removed_col = i+1
    
    if reviews_col is None or removed_col is None:
        console.log(f"failed to find columns in {reviews_sheet_link}",style="red")
        return []
    
    all_reviews = table['Link to Review'].to_list()
    all_removed = table['Removed'].to_list()

    report = []

    for i in range(0,len(all_reviews)):
        if all_removed[i] == "FALSE":
            status = check_status(all_reviews[i])
            if status == 404:
                report.append(
                    {
                        "Review ID": table.iloc[i]["Review ID"],
                        "Review Link":all_reviews[i],
                        "Removed": True,
                        "Sheet":reviews_sheet_link
                    }
                )
                all_removed[i] = "TRUE"

    sheet.update(f'{chr(64 + removed_col)}:{chr(64 + removed_col)}', [["Removed"]]+[ [False] if r == "FALSE" else [True] for r in all_removed])

    console.log(f"found [{len(report)}] review removed {all_reviews[i]}",style="green")

    generate_report(reviews_sheet_link,spread_sheet , report)
    
    console.log(f"{reviews_sheet_link} completed!",style="blue")
    return report



def main():
    console.log(f"Getting all the reviews sheet links from master",style="purple")
    monitering_reviews_link = get_reviews_link()
    console.log(f"found [{len(monitering_reviews_link)}] links from the masters sheet.",style="blue")

    report = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = executor.map(update_reviews_removed,monitering_reviews_link)
        for r in results:
            report.extend(r)
        report = pd.DataFrame(report)
    report.to_csv("report.csv")

    console.log(f"Update Done!!",style="Green")



if __name__ == '__main__':
    main()









