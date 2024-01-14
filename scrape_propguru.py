import json
import requests
import urllib3
import concurrent.futures
from tqdm import tqdm
import random
import time
import datetime
import os
import botHAR
from bs4 import BeautifulSoup
import argparse

# Configurations
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def delete_files_in_directory(directory_path):
    # Check if the directory exists
    if os.path.exists(directory_path) and os.path.isdir(directory_path):
        # Iterate over the files in the directory
        for file_name in os.listdir(directory_path):
            file_path = os.path.join(directory_path, file_name)
            try:
                # Check if it is a file (not a directory)
                if os.path.isfile(file_path) and not file_name.startswith('.'):
                    # Remove the file
                    os.remove(file_path)
                    print(f"Deleted file: {file_path}")
            except Exception as e:
                print(f"Failed to delete file {file_path}. Error: {str(e)}")
    else:
        print(f"Directory not found: {directory_path}")

def validateSession(is_bot):
    if is_bot:
        print('Starting bot...')
        if botHAR.main(path_to_chrome):
            print('HAR DOWNLOAD: SUCCESS')
            
            print(f'...HAR exported to www.propertyguru.com.sg.har')
        else:
            print('HAR DOWNLOAD: FAIL')
            return False
    else:
     
        notif = """FOLLOW THESE STEPS AND CONTINUE WHEN DONE:
                # Step by step:
                # 1. Open browser
                # 2. Go to https://www.propertyguru.com.sg/property-for-sale/20?
                # 3. Solve the CAPTCHA as usual
                # 4. Once page loads, Ctl-Shift-I to open Inspect
                # 5. Go to URL bar and press Enter on the same URL
                # 5. Go to Inspect > Network > Click on gear icon on top left
                # 6. Click on the download icon 'Export HAR file'
                # 7. Save it to the root directory of this repo. File name should be left as 'www.propertyguru.com.sg.har'
                """   
        print(notif)

    
    return True

def getHeaders(path_to_har):
    with open(path_to_har,'r',encoding="utf8") as har:
        logs = json.loads(har.read())
        entries =  logs['log']['entries']
    target = 'https://www.propertyguru.com.sg/property-for-sale/20?' 
    #target = 'https://www.propertyguru.com.sg/property-for-sale/21?property_type=H&property_type_code%5B0%5D=6J&property_type_code%5B1%5D=EA&property_type_code%5B2%5D=EM&search=true'
    for i,entry in enumerate(entries):
        if entry['request']['url'].startswith(target) and entry['request']['method'] == 'GET':
            print('Valid GET request')
            break
                
    headers_raw = entry['request']['headers']
    
    exclude_headers = [':authority',':method',':path',':scheme']
    headers = {}
    for header in headers_raw:
        if header["name"] not in exclude_headers:
            headers[header["name"]] = header["value"]
    return headers

def testConn(headers):
    session = requests.Session()
    print('Test connection:')
    test_url = "https://www.propertyguru.com.sg/property-for-sale/21?"
    #test_url = 'https://www.propertyguru.com.sg/property-for-sale/21?property_type=H&property_type_code%5B0%5D=6J&property_type_code%5B1%5D=EA&property_type_code%5B2%5D=EM&search=true'
    resp = session.get(test_url, headers = headers, verify=False)
    if resp.status_code == 200:
        print('... valid cookies')
    else:
        print('...failed')
        #exit(1)
        
def getPages(headers):
    r = requests.get('https://www.propertyguru.com.sg/property-for-sale',headers=headers, verify = False)
    if r.status_code == 200:
        
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Find all elements with class 'pagination'
        pagination_elements = soup.find_all(class_='pagination')
        # Extract numbers from the 'pagination' elements and find the maximum value
        numbers = [int(link.get('data-page', 0)) for element in pagination_elements for link in element.find_all('a') if link.get('data-page').isdigit()]

        if numbers:
            max_number = max(numbers)
            print(f"# pages: {max_number}")       
        return max_number

def truncateHTML(response):
    
    html = response.content
    # Keep only the var guruApp and the 20 listings per html page
    soup = BeautifulSoup(html, 'html.parser')
    summary_data = soup.find('script', text=lambda x: x and 'var guruApp' in x)
    main_body = soup.find(id="listings-container")
    
    # Create a new html
    combined_soup = BeautifulSoup('', 'html.parser')
    combined_soup.append(summary_data)
    combined_soup.append(main_body)
    return combined_soup.prettify()

def process_item(item):
    # This function represents the task you want to perform on each item
    # Modify this function based on your specific requirements
    n = item
    time.sleep(6*random.random())
    session = requests.Session()
    url = f"https://www.propertyguru.com.sg/property-for-sale/{str(n)}?"
    #url = f"https://www.propertyguru.com.sg/property-for-sale/{str(n)}?property_type=H&property_type_code[]=5A&property_type_code[]=5I&property_type_code[]=5PA&property_type_code[]=5S&search=true"
    response = session.get(url, headers = headers, verify=False)
    
    #print(f"Processed item: {item}, Result: {response}")
    with open(f'{dirpath}/page_{n}.html', 'w', encoding='utf-8') as file:
        file.write(truncateHTML(response))
        #file.write(response.text)
    return response

def parallel_process(items, num_workers):
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        results = list(tqdm(executor.map(process_item, items), total=len(items)))
        return results
    
def get_directory_size(directory_path):
    total_size = 0

    for dirpath, dirnames, filenames in os.walk(directory_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += os.path.getsize(file_path)

    # Convert total_size to kilobytes (optional)
    total_size_mb = total_size / (1024.0*1024)

    return total_size_mb

def log_run(dirpath):
    with open(f'{dirpath}/run_report.txt', 'w', encoding='utf-8') as file:
        file.write(f"Start: {start_t} \n")
        file.write(f"End: {datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')} \n")
        file.write(f"Pages: {TOTAL_PAGES} \n")
        file.write(f"Total Size (MB): {get_directory_size(dirpath)}\n")
        
if __name__ == "__main__":
 
    parser = argparse.ArgumentParser(description='Runs the full scrape and download.')
    parser.add_argument('--test', action='store_true', help='Flag to turn on test mode, limit pages to 25')
    args = parser.parse_args()
    
    with open("config.json") as config_file:
        config_data = json.load(config_file)
    start_t = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')
    dirpath = config_data["path_to_data_htmls"]
    try:
        os.makedirs(dirpath)
    except FileExistsError as e:
        print(e)
        
    path_to_chrome = f'{config_data["path_to_chrome"]} %s --incognito'
    delete_files_in_directory(dirpath)
    #IS_BOT = int(input("Use bot to get HAR file?: 1/0 for Yes/No"))
    IS_BOT = 1
            
    
    if not IS_BOT:
        HAR_SAVED = int(input("Have you saved the HAR file in this directory? -- 1/0 for Yes/No: "))
    
    else:
        har_saved = validateSession(IS_BOT)
        HAR_SAVED=har_saved
    
    har_dir = f'{config_data["path_to_har"]}/www.propertyguru.com.sg.har'
    
    if HAR_SAVED:
        headers = getHeaders(har_dir)
        print(headers)
        testConn(headers)
        TOTAL_PAGES = getPages(headers)
        if args.test:
            TOTAL_PAGES = 25

        my_list = [i for i in range(1,TOTAL_PAGES+1,1)]
        num_parallel_workers = 8

        # Perform parallel processing
        results = parallel_process(my_list, num_parallel_workers)
        log_run(dirpath)
        
    else:
        print('No HAR file, exiting...')
        exit(1)        