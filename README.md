# About

Python web scraper and sentiment analyzer for company filings found in the U.S Securities And Exchange Commission database EDGAR.

# Usage
  * pip install -r requirements.txt
  * python thor.py --help
  * Fetch and store 10-Q report URLs in local database `python thor.py fetch-report-urls`
  * Analyze the reports by running `python thor.py analyze`
  * Download the full index database and save it in the project directory. Change the database url in `db_connect` respectively.

You only need to fetch and store the report URLs once. Modify the `analyze` methods to fit your needs and run `python thor.py analyze` to update the results.
