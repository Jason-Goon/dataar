import os
import csv
import requests
from googlesearch import search
from bs4 import BeautifulSoup
import time

def is_valid_website(url):
    try:
        print(f"Checking website: {url}")
        response = requests.get(url, timeout=10)
        if response.status_code == 404:
            print("Website returned 404, keeping the company.")
            return False
       
        soup = BeautifulSoup(response.content, 'html.parser')
        body_text = soup.get_text(strip=True)
        if len(body_text) > 100:
            print("Website is valid and has content.")
            return True
        else:
            print("Website has minimal content.")
            return False
    except requests.RequestException as e:
        print(f"Error accessing {url}: {e}, keeping the company.")
        return False

def find_website(company_name):
    print(f"Searching for website for company: {company_name}")
    query = f"{company_name} official site"
    try:
        for url in search(query, num_results=5, lang="en"):
            if is_valid_website(url):
                return url
        return None
    except Exception as e:
        print(f"Error during search for {company_name}: {e}")
        return None

def process_batch(companies_batch, output_dir):
    companies_to_keep = []
    seen_companies = set() 

    for company in companies_batch:
        company_name = company.get('company_name')

        if company_name in seen_companies:
            continue
        seen_companies.add(company_name)

        website = company.get('website')
        print(f"\nCompany: {company_name}")
        if not website:
            website = find_website(company_name)
            if website:
                company['website'] = website

        print(f"Website: {website if website else 'No website found'}")
        companies_to_keep.append(company)

    output_file = os.path.join(output_dir, f"processed_batch_{time.time()}.csv")
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['Y-tunnus', 'company_name', 'address', 'website', 'registrationDate', 'detailsUri', 'companyForm']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(companies_to_keep)
    
    print(f"Finished processing batch. Results saved to {output_file}.")

def main():
    input_dir = os.getcwd()  # Current working directory
    output_dir = os.path.join(input_dir, "processed_files")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_companies = []
    for filename in os.listdir(input_dir):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_dir, filename)
            with open(file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                all_companies.extend(list(reader))

    # Process in batches of 50
    batch_size = 50
    for i in range(0, len(all_companies), batch_size):
        batch = all_companies[i:i + batch_size]
        process_batch(batch, output_dir)

        # Respect rate limits by sleeping between batches
        print("Sleeping for 2 minutes to avoid rate limiting...")
        time.sleep(120)  # Sleep for 2 minutes

if __name__ == "__main__":
    main()
