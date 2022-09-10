For setup:
1. you will need to create a project in the google api console
2. then you will have to go to "Enable api and services" and activate "google drive API" and "google sheets API"
3. after that you will have to go to "oauth conscent screen" and set that up
4. then you will need to go to "credentials" and press on "+ create credentials" and create a "OAuth Client ID" but you will have to make sure that its a desktop application
5. after that download the json file and add it to root folder googlesheets and rename it to "credentials.json"



to run the script:
1. env/scripts/activate.ps1
2. python -m update_removed_reviews.py
