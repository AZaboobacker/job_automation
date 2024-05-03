import os
import json
import pandas as pd
from bs4 import BeautifulSoup
import requests
import schedule
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Specify your skill set
my_skills = ['Python', 'Data Analysis', 'Machine Learning', 'SQL', 'Communication']

# Function to scrape job listings from a job portal
def scrape_job_listings(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Your scraping code to extract job listings here
    # Example:
    job_listings = []
    for listing in soup.find_all('div', class_='job_listing'):
        job_title = listing.find('h2', class_='job_title').text.strip()
        company_name = listing.find('span', class_='company_name').text.strip()
        location = listing.find('span', class_='location').text.strip()
        job_description = listing.find('div', class_='job_description').text.strip()
        post_date = listing.find('span', class_='post_date').text.strip()

        # Check if the job listing already exists in the Google Sheets
        if not job_exists_in_sheets(job_title, company_name):
            all_skills = [skill.strip() for skill in job_description.split(',')]

            matched_skills = [skill for skill in all_skills if skill in my_skills]
            unmatched_skills = [skill for skill in all_skills if skill not in my_skills]

            # Get pay or salary information if available
            pay_salary = listing.find('span', class_='pay_salary')
            pay_salary_text = pay_salary.text.strip() if pay_salary else 'N/A'

            job_listings.append({
                'Job Title': job_title,
                'Company Name': company_name,
                'Location': location,
                'Job Description': job_description,
                'Post Date': post_date,
                'Matched Skills': ', '.join(matched_skills) if matched_skills else 'No matching skills',
                'Unmatched Skills': ', '.join(unmatched_skills) if unmatched_skills else 'No unmatched skills',
                'Pay/Salary': pay_salary_text
            })
    return job_listings

# Function to check if a job listing already exists in Google Sheets
def job_exists_in_sheets(job_title, company_name):
    # Authenticate with Google Sheets API
    credentials_dict = {
        "type": "service_account",
        "project_id": os.environ.get('GOOGLE_PROJECT_ID'),
        "private_key_id": os.environ.get('GOOGLE_PRIVATE_KEY_ID'),
        "private_key": os.environ.get('GOOGLE_PRIVATE_KEY').replace('\\n', '\n'),
        "client_email": os.environ.get('GOOGLE_CLIENT_EMAIL')
    }
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)

    # Use the credentials to access Google Sheets API
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()

    # Check if the job title and company name exist in the worksheet
    try:
        result = sheet.values().get(spreadsheetId=os.environ.get('GOOGLE_SHEET_ID'), 
                                    range='Sheet1').execute()
        values = result.get('values', [])
        if not values:
            return False
        for row in values[1:]:
            if row[0] == job_title and row[1] == company_name:
                return True
        return False
    except Exception as e:
        print("Error:", e)
        return False

# Function to process scraped data and save it to Google Sheets
def process_data_and_save_to_sheets(data):
    # Authenticate with Google Sheets API
    credentials_dict = {
        "type": "service_account",
        "project_id": os.environ.get('GOOGLE_PROJECT_ID'),
        "private_key_id": os.environ.get('GOOGLE_PRIVATE_KEY_ID'),
        "private_key": os.environ.get('GOOGLE_PRIVATE_KEY').replace('\\n', '\n'),
        "client_email": os.environ.get('GOOGLE_CLIENT_EMAIL')
    }
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)

    # Use the credentials to access Google Sheets API
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()

    # Append data to the worksheet
    values = [['Job Title', 'Company Name', 'Location', 'Job Description', 'Post Date', 'Matched Skills', 'Unmatched Skills', 'Pay/Salary']]
    for job in data:
        values.append([job['Job Title'], job['Company Name'], job['Location'],
                       job['Job Description'], job['Post Date'], job['Matched Skills'],
                       job['Unmatched Skills'], job['Pay/Salary']])
    body = {'values': values}
    try:
        result = sheet.values().append(spreadsheetId=os.environ.get('GOOGLE_SHEET_ID'), 
                                       range='Sheet1', 
                                       valueInputOption='RAW', 
                                       body=body).execute()
        print(f"Data added: {result.get('updates').get('updatedCells')} cells updated.")
    except Exception as e:
        print("Error:", e)

# Function to check for new job listings and update Google Sheets
def update_sheets_with_new_listings():
    # Scrape job listings from each job portal
    new_data = []
    for portal, url in job_portals.items():
        job_listings = scrape_job_listings(url)
        new_data.extend(job_listings)

    # Append new job listings to Google Sheets
    if new_data:
        process_data_and_save_to_sheets(new_data)

# Define job portals to scrape
job_portals = {
    'LinkedIn': 'https://www.linkedin.com/jobs',
    'Indeed': 'https://www.indeed.com',
    'ZipRecruiter': 'https://www.ziprecruiter.com',
    # Add more job portals as needed
}

# Main function to initiate scraping and updating Google Sheets
def main():
    update_sheets_with_new_listings()

# Schedule scraping and updating process
schedule.every().day.at("08:00").do(main)  # Adjust the time as needed

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute
