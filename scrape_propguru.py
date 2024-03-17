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

# Local
import filter_url_param_config

# Configurations
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Functions
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
    target = 'https://www.propertyguru.com.sg/property-for-sale/25?property_type=H&property_type_code[]=1R&property_type_code[]=2A&property_type_code[]=2I&property_type_code[]=2S&property_type_code[]=3A&property_type_code[]=3NG&property_type_code[]=3Am&property_type_code[]=3NGm&property_type_code[]=3I&property_type_code[]=3Im&property_type_code[]=3S&property_type_code[]=3STD&property_type_code[]=3PA&property_type_code[]=4A&property_type_code[]=4PA&property_type_code[]=4NG&property_type_code[]=5A&property_type_code[]=4STD&property_type_code[]=4I&property_type_code[]=4S&property_type_code[]=5I&property_type_code[]=5PA&property_type_code[]=5S&property_type_code[]=6J&property_type_code[]=EA&property_type_code[]=EM&property_type_code[]=MG&property_type_code[]=TE&search=true' 
    #target = 'https://www.propertyguru.com.sg/property-for-sale/21?property_type=H&property_type_code%5B0%5D=6J&property_type_code%5B1%5D=EA&property_type_code%5B2%5D=EM&search=true'
    for i,entry in enumerate(entries):
        if entry['request']['url'].startswith(target) and entry['request']['method'] == 'GET':
            print(f'Valid GET request at entry index {i}')
            break
        else:
            raise ValueError("ERROR: Could not find header in HAR!")
                
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
    test_url = "https://www.propertyguru.com.sg/property-for-sale/26?property_type=H&property_type_code[]=1R&property_type_code[]=2A&property_type_code[]=2I&property_type_code[]=2S&property_type_code[]=3A&property_type_code[]=3NG&property_type_code[]=3Am&property_type_code[]=3NGm&property_type_code[]=3I&property_type_code[]=3Im&property_type_code[]=3S&property_type_code[]=3STD&property_type_code[]=3PA&property_type_code[]=4A&property_type_code[]=4PA&property_type_code[]=4NG&property_type_code[]=5A&property_type_code[]=4STD&property_type_code[]=4I&property_type_code[]=4S&property_type_code[]=5I&property_type_code[]=5PA&property_type_code[]=5S&property_type_code[]=6J&property_type_code[]=EA&property_type_code[]=EM&property_type_code[]=MG&property_type_code[]=TE&search=true"
    #test_url = 'https://www.propertyguru.com.sg/property-for-sale/21?property_type=H&property_type_code%5B0%5D=6J&property_type_code%5B1%5D=EA&property_type_code%5B2%5D=EM&search=true'
    resp = session.get(test_url, headers = headers, verify=False)
    if resp.status_code == 200:
        print('... valid cookies')
    else:
        print('...failed')
        #exit(1)
        
def getPages(headers,filter_url_params):
    r = requests.get(f'https://www.propertyguru.com.sg/property-for-sale?{filter_url_params}',headers=headers, verify = False)
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
    #print(response.content)
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
    name = item["name"]
    page = item["page"]
    url = item["url"]
    #print(url)
    s1 = time.time()
    time.sleep(2*random.random())
    s2 = time.time()
    #session = requests.Session() # DO NOT START A NEW SESSION
    #url = f"https://www.propertyguru.com.sg/property-for-sale/{str(n)}?"
    #url = f"https://www.propertyguru.com.sg/property-for-sale/{str(n)}?property_type=H&property_type_code[]=5A&property_type_code[]=5I&property_type_code[]=5PA&property_type_code[]=5S&search=true"
    #response = session.get(url, headers = headers, verify=False)
    g1 = time.time()
    response = requests.get(url, headers = headers, verify=False)
    g2 = time.time()

    if 'Bot Protection' in response.text:
        raise ValueError(f"ERROR: Hit bot protection on PAGE <{page}> | URL <{url}>")
    #print(f"Processed item: {item}, Result: {response}")
    w1 = time.time()

    with open(f'{dirpath}/{name}_page_{page}.html', 'w', encoding='utf-8') as file:
        file.write(truncateHTML(response))
        #file.write(response.text)
    w2 = time.time()
    # if page %10 == 0:
    #     print(f'{name}_{page} | SLEEP: {(s2 -s1) :.4f}s |GET: {(g2 -g1) :.4f}s | WRITE: {(w2 -w1) :.4f}s')
    return None

def parallel_process(items, num_workers):
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        tqdm(executor.map(process_item, items), total=len(items))
        return None
    
def get_directory_size(directory_path, filter_url_name):
    total_size = 0

    for dirpath, dirnames, filenames in os.walk(directory_path):
        for filename in filenames:
            if filter_url_name in filename:
                file_path = os.path.join(dirpath, filename)
                total_size += os.path.getsize(file_path)

    # Convert total_size to kilobytes (optional)
    total_size_mb = total_size / (1024.0*1024)

    return total_size_mb

def log_run(dirpath, filter_url_name):
    with open(f'{dirpath}/0_{filter_url_name}_run_report.txt', 'w', encoding='utf-8') as file:
        file.write(f"Start: {start_t} \n")
        file.write(f"End: {datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')} \n")
        file.write(f"Pages: {TOTAL_PAGES} \n")
        file.write(f"Total Size (MB): {get_directory_size(dirpath,filter_url_name)}\n")
        
if __name__ == "__main__":
 
    #parser = argparse.ArgumentParser(description='Runs the full scrape and download.')
    #parser.add_argument('--test', action='store_true', help='Flag to turn on test mode, limit pages to 25')
    #args = parser.parse_args()
    
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
        #har_saved = validateSession(IS_BOT)
        #HAR_SAVED=har_saved
        HAR_SAVED=1
    
    har_dir = f'{config_data["path_to_har"]}/www.propertyguru.com.sg.har'
    
    if HAR_SAVED:
        headers = getHeaders(har_dir)
        print(headers)
        testConn(headers)
        
        ### Allows specific URL params
        for filter_url in filter_url_param_config.ENABLED_FILTERS:
            filter_url_name = filter_url['name']
            filter_url_params = filter_url['params']
            TOTAL_PAGES = getPages(headers, filter_url_params)
            if config_data['test_run']:
                if TOTAL_PAGES > 25:
                    TOTAL_PAGES = 25
            
            my_list = [i for i in range(1,TOTAL_PAGES+1,1)]
            
            base_url = "https://www.propertyguru.com.sg/property-for-sale/"
            url_list = [{"name":filter_url_name,"page":i,"url": f"{base_url}{i}?{filter_url_params}/"} for i in range(1,TOTAL_PAGES+1,1)]
            
            num_parallel_workers = 4

            # Perform parallel processing
            parallel_process(url_list, num_parallel_workers) # TURNED OFF PARALLEL PROCESSING
            
            session = requests.Session()
            # for item in tqdm(url_list):
            #     process_item(item, session, headers)
            log_run(dirpath, filter_url_name)
        
    else:
        print('No HAR file, exiting...')
        exit(1)        