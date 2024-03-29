import json
import requests
import urllib3
import concurrent.futures
from tqdm import tqdm
import random
import time
import datetime
import os
import botHAR_rpi
from bs4 import BeautifulSoup
import argparse
import logging

# Create a logger object
logger = logging.getLogger('SCRAPER')
logger.setLevel(logging.DEBUG)  # Set the logging level to DEBUG

# Create a file handler which logs even debug messages
fh = logging.FileHandler(f"log/pipeline_{datetime.datetime.utcnow().date().strftime('%Y%m%d')}.log")
fh.setLevel(logging.DEBUG)

# Create a console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)  # Only log errors and above to the console

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)



logger.info(f'START RUN: {datetime.datetime.utcnow()}')

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
                    #print(f"Deleted file: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to delete file {file_path}. Error: {str(e)}")
    else:
        logger.warning(f"Directory not found: {directory_path}")

def validateSession(path_to_chrome, target_url = None):

    logger.info('Starting bot...')
    if botHAR_rpi.main(path_to_chrome, target_url):
        logger.info('HAR DOWNLOAD: SUCCESS')
        return True
    else:
        logger.warning('HAR DOWNLOAD: FAIL')
        return False


def getHeaders(path_to_har):
    with open(path_to_har,'r',encoding="utf8") as har:
        logs = json.loads(har.read())
        entries =  logs['log']['entries']
    target = 'https://www.propertyguru.com.sg/property-for-sale/20?'
    target = 'https://www.propertyguru.com.sg/property-for-sale/25?property_type=H&property_type_code[]=1R&property_type_code[]=2A&property_type_code[]=2I&property_type_code[]=2S&property_type_code[]=3A&property_type_code[]=3NG&property_type_code[]=3Am&property_type_code[]=3NGm&property_type_code[]=3I&property_type_code[]=3Im&property_type_code[]=3S&property_type_code[]=3STD&property_type_code[]=3PA&property_type_code[]=4A&property_type_code[]=4PA&property_type_code[]=4NG&property_type_code[]=5A&property_type_code[]=4STD&property_type_code[]=4I&property_type_code[]=4S&property_type_code[]=5I&property_type_code[]=5PA&property_type_code[]=5S&property_type_code[]=6J&property_type_code[]=EA&property_type_code[]=EM&property_type_code[]=MG&property_type_code[]=TE&search=true' 
    #target = 'https://www.propertyguru.com.sg/property-for-sale/21?property_type=H&property_type_code%5B0%5D=6J&property_type_code%5B1%5D=EA&property_type_code%5B2%5D=EM&search=true'
    for i,entry in enumerate(entries):
        if entry['request']['url'].startswith(target) and entry['request']['method'] == 'GET':
            logger.info(f'Valid GET request at entry index {i}')
            break
        else:
            raise ValueError("ERROR: Could not find header in HAR!")
                
    headers_raw = entry['request']['headers']
    exclude_headers = [':authority',':method',':path',':scheme']
    headers = {}
    for header in headers_raw:
        if header["name"] not in exclude_headers:
            headers[header["name"]] = header["value"]
    
    cookies = entry['request']['cookies']
    for cookie in cookies:
        if cookie.get("name") == '__cf_bm':
            cookie_expiry =cookie['expires'].split(".")[0]
            logger.info(f'CF COOKIE EXPIRES: {cookie_expiry}')
    cookie_expiry_ts = datetime.datetime.strptime(cookie_expiry, "%Y-%m-%dT%H:%M:%S")        
    
    return {'headers':headers, 'cookie_expiry_ts':cookie_expiry_ts}

def testConnPass(headers):
    session = requests.Session()
    logger.info('Test connection:')
    test_url = "https://www.propertyguru.com.sg/property-for-sale/21?"
    test_url = "https://www.propertyguru.com.sg/property-for-sale/26?property_type=H&property_type_code[]=1R&property_type_code[]=2A&property_type_code[]=2I&property_type_code[]=2S&property_type_code[]=3A&property_type_code[]=3NG&property_type_code[]=3Am&property_type_code[]=3NGm&property_type_code[]=3I&property_type_code[]=3Im&property_type_code[]=3S&property_type_code[]=3STD&property_type_code[]=3PA&property_type_code[]=4A&property_type_code[]=4PA&property_type_code[]=4NG&property_type_code[]=5A&property_type_code[]=4STD&property_type_code[]=4I&property_type_code[]=4S&property_type_code[]=5I&property_type_code[]=5PA&property_type_code[]=5S&property_type_code[]=6J&property_type_code[]=EA&property_type_code[]=EM&property_type_code[]=MG&property_type_code[]=TE&search=true"
    #test_url = 'https://www.propertyguru.com.sg/property-for-sale/21?property_type=H&property_type_code%5B0%5D=6J&property_type_code%5B1%5D=EA&property_type_code%5B2%5D=EM&search=true'
    resp = session.get(test_url, headers = headers, verify=False)
    if resp.status_code == 200:
        logger.info('... valid cookies')
        return True
    else:
        logger.info('...failed')
        return False
        
def getPages(headers,url):
    r = requests.get(url,headers=headers, verify = False)
    if r.status_code == 200:
        
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Find all elements with class 'pagination'
        pagination_elements = soup.find_all(class_='pagination')
        # Extract numbers from the 'pagination' elements and find the maximum value
        numbers = [int(link.get('data-page', 0)) for element in pagination_elements for link in element.find_all('a') if link.get('data-page').isdigit()]

        if numbers:
            max_number = max(numbers)
            logger.info(f"# pages: {max_number}")       
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

def process_item(item, session, headers):
    # This function represents the task you want to perform on each item
    # Modify this function based on your specific requirements
    name = item["name"]
    page = item["page"]
    url = item["url"]
    #print(url)
    s1 = time.time()
    #time.sleep(1*random.random())
    s2 = time.time()
    #session = requests.Session() # DO NOT START A NEW SESSION
    #url = f"https://www.propertyguru.com.sg/property-for-sale/{str(n)}?"
    #url = f"https://www.propertyguru.com.sg/property-for-sale/{str(n)}?property_type=H&property_type_code[]=5A&property_type_code[]=5I&property_type_code[]=5PA&property_type_code[]=5S&search=true"
    #response = session.get(url, headers = headers, verify=False)
    g1 = time.time()
    response = requests.get(url, headers = headers, verify=False)
    g2 = time.time()

    if 'Bot Protection' in response.text:
        logging.warning(f"WARNING: Returning FALSE - Hit bot protection on PAGE <{page}> | URL <{url}> \n\n RESPONSE:: \n {response.text}")
        return False
    #print(f"Processed item: {item}, Result: {response}")
    w1 = time.time()

    with open(f'{dirpath}/{name}_page_{page}.html', 'w', encoding='utf-8') as file:
        file.write(truncateHTML(response))
        #file.write(response.text)
    w2 = time.time()
    if page %10 == 0:
        logger.info(f'{name}_{page} | SLEEP: {(s2 -s1) :.4f}s |GET: {(g2 -g1) :.4f}s | WRITE: {(w2 -w1) :.4f}s')
    return True

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

def log_run(start_t, filter_url_name):
    # with open(f'{dirpath}/0_{filter_url_name}_run_report.txt', 'w', encoding='utf-8') as file:
    #     file.write(f"Start: {start_t} \n")
    #     file.write(f"End: {datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')} \n")
    #     file.write(f"Pages: {TOTAL_PAGES} \n")
    #     file.write(f"Total Size (MB): {get_directory_size(dirpath,filter_url_name)}\n")
    logger.info(f"QUERY: {filter_url_name} | Start: {start_t} \n")
    logger.info(f"QUERY: {filter_url_name} | End: {datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')} \n")
    logger.info(f"QUERY: {filter_url_name} | Pages: {TOTAL_PAGES} \n")
        
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
        logger.error(e)
        
    path_to_chrome = f'{config_data["path_to_chromium_browser"]} %s --incognito'
    delete_files_in_directory(dirpath)
    #IS_BOT = int(input("Use bot to get HAR file?: 1/0 for Yes/No"))

    # Validate session for first time
    validateSession(path_to_chrome) 
    
    # Check if HAR is up to date
    har_dir = f'{config_data["path_to_har"]}/www.propertyguru.com.sg.har'
    har_extract = getHeaders(har_dir)
    headers = har_extract['headers']
    cookie_expiry_ts = har_extract['cookie_expiry_ts']
    #print(headers)
    #print(cookie_expiry_ts )
    ### Allows specific URL params
    for filter_url in filter_url_param_config.ENABLED_FILTERS:
        
        
        
        start_t = datetime.datetime.utcnow()
        filter_url_name = filter_url['name']
        filter_url_params = filter_url['params']
    
        # if 'LANDED' in filter_url_name: # stagger for 60 mins to cool off before the last scrape
        #     logger.info("PAUSING SCRAPE for 60 mins")
        #     time.sleep(60*60)
        #     logger.info("WAKING UP AND VALIDATING...")
        #     validateSession(path_to_chrome) 
        #     har_dir = f'{config_data["path_to_har"]}/www.propertyguru.com.sg.har'
        #     har_extract = getHeaders(har_dir)
        #     headers = har_extract['headers']
        #     cookie_expiry_ts = har_extract['cookie_expiry_ts']
        pages_url = f'https://www.propertyguru.com.sg/property-for-sale?{filter_url_params}'
        TOTAL_PAGES = getPages(headers, pages_url)
        if TOTAL_PAGES > 400:
            PAGE_CHECK_THRESHOLD=20 # Assume max 5% variation in pages
        else:
            PAGE_CHECK_THRESHOLD=5
        #TOTAL_PAGES = 
        if config_data['test_run']:
            if TOTAL_PAGES > 25:
                TOTAL_PAGES = 25
        
        my_list = [i for i in range(1,TOTAL_PAGES+1,1)]
        
        base_url = "https://www.propertyguru.com.sg/property-for-sale/"
        url_list = [{"name":filter_url_name,"page":i,"url": f"{base_url}{i}?{filter_url_params}/"} for i in range(1,TOTAL_PAGES+1,1)]
        
        

        # Perform parallel processing
        # num_parallel_workers = 4
        # parallel_process(url_list, num_parallel_workers)
        testConnPass(headers)
        session = requests.Session()
        prev_item = None
        for item in tqdm(url_list):
            # Check if the cookie is expiring soon
            if (cookie_expiry_ts - datetime.datetime.utcnow()) < datetime.timedelta(minutes=5):
                logger.info(f"TTL: {cookie_expiry_ts - datetime.datetime.utcnow()}, re-validating...")
                retries = 0
                test_pass = False
                while (test_pass == False) and (retries < 3):
                    if retries == 3:
                        logger.error(f"MAXIMUM CAPTCHA VALIDATION RETRIES EXCEEDED. EXITING AT {datetime.datetime.utcnow()}...")
                        exit(1)
                        
                    logger.info(f'RE-VAL: retries={retries}')
                    validateSession(path_to_chrome)
                    har_extract = getHeaders(har_dir)
                    headers = har_extract['headers']
                    cookie_expiry_ts = har_extract['cookie_expiry_ts']
                    test_pass = testConnPass(headers)
                    retries += 1
                logger.info(f'RE-VAL: test_pass={test_pass}')
            else:
                # Then check if the page we are going to request is still valid\
                if TOTAL_PAGES - item["page"] < PAGE_CHECK_THRESHOLD: # When we are getting close to the end
                    logger.info("Approaching end of pagination! Checking total pages.")
                    if not prev_item == None:
                        latest_total_pages = getPages(headers,prev_item["url"])
                    if item["page"] > latest_total_pages:
                        logger.warning("TOTAL_PAGES changed. Initial: {TOTAL_PAGES} | Latest: {latest_total_pages}. BREAKING QUERY: {filter_url_name}")
                        break
                else:
                    # Finally, try to process the url, and handle the bot protection page if it gets triggered.
                    process_retries = 0 
                    while not process_item(item, session, headers): # returns False when Bot Detection is found
                        logger.warning("WARNING: BOT DETECTION PAGE - RETRYING REQUEST...")
                        process_retries += 1
                        
                        if process_retries == 3:
                            logger.error(f"MAXIMUM CAPTCHA VALIDATION RETRIES EXCEEDED FOR {item['url']}. EXITING AT {datetime.datetime.utcnow()}...")
                            exit(1)
                        
                        # Validate the session and try to update the headers.
                        validateSession(path_to_chrome, item['url'] ) # Important to use the ACTUAL url that we are getting rejected on.
                        har_dir = f'{config_data["path_to_har"]}/www.propertyguru.com.sg.har'
                        har_extract = getHeaders(har_dir)
                        headers = har_extract['headers']
            prev_item = item    
                            

        log_run(dirpath, filter_url_name)
        
