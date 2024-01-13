
# This script takes all files in the html-files folder and extracts the summarised listing data into a dataframe, 1 row per listing. It is meant as a PoC to show how the listing data can be utilised. There is much richer data in the HTML which can be further extracted.

import json
import pandas as pd
from bs4 import BeautifulSoup
import os
from tqdm import tqdm
import datetime
import traceback

PATH_TO_HTMLS = input("Path to directory, relative to this main app directory. e.g. data/xx-htmls")
SAVE_FILE_AS = input("File name WITHOUT the extension. File will be saved as <FILENAME>_df.csv -- OPTIONAL: If empty, will use timestamp of the html files")

def parseSummary(html):
    
    soup = BeautifulSoup(html, 'html.parser')
    script_tags = soup.find_all('script') # PropGuru preforms the JS script containing the listing data that will be displayed on the page. The summary data is stored as textual JS.
    
    for script in script_tags:
        if 'listingResultsWidget' in str(script): # This is the section containing our listings
            raw = script
            raw_str = raw.text # Needed to extract the listings as strings


    # Since the entire dictionary is a string, we need to recreate the dict
    start = raw_str.index('{')
    end = raw_str.rfind('var dataLayer')
    raw_text = raw_str[start:end]
    #print(raw_text)
    end = raw_text.rfind(';')
    raw_text = raw_text[:end]
    #print(raw_text)
    raw_text = raw_text.replace('\'gaECListings\'','\"gaECListings\"')
    json_obj = json.loads(raw_text)
    
    # Go through each listing format all data into a df
    df_ls = []
    for item in json_obj['listingResultsWidget']['gaECListings']:
        productData = item['productData']
        df_ls.append(pd.DataFrame.from_dict(productData, orient='index').T)
    page_data = pd.concat(df_ls)
    page_data['id'] = page_data['id'].astype(int)
    return page_data

def parseDetail(html):

    soup = BeautifulSoup(html, 'html.parser')
    elements = soup.find_all(class_="listing-card")

    # Now, you can iterate through the elements and access the HTML content
    #print('ELEMENTS',len(elements))
    listing_datas = []
    for i,element in enumerate(elements):
        #print('LISTING:', i+1)
        listing_element = element

        # Set some defaults incase there are issues:

        try:
            proximity_mrt = listing_element.select_one('.listing-description .pgicon-walk').next_sibling.strip()
        except:
            proximity_mrt = None           
        
        try: 
            headline = listing_element.select_one('.headline').find_all('div')[3].text.strip()
        except:
            headline = None
        
        try:
            beds = listing_element.select_one('.listing-description .listing-rooms .bed').text.strip()
            baths = listing_element.select_one('.listing-description .listing-rooms .bath').text.strip()
        except:
            beds = None
            baths = None
        
        try:
            agent_id = listing_element.select_one('.headline').find_all('a')[1]['data-agent-id']
        except:
            agent_id = None
            
        try:
            agent_name = listing_element.select_one('.agent-name .name').text.strip()
            agent_link = listing_element.select_one('.agent-name ').find('a')['href']
        except:
            agent_name = None
            agent_link = None
        
        try: 
            price = listing_element.select_one('.listing-description .list-price .price').text.strip()
        except:
            price = None
        
        # Extract data from the listing element
        
        try:
            area = listing_element.select_one('.listing-description .listing-floorarea').text.strip()
        except:
            area = None
        
        cover_image = None
        for attr in ['content','data-original','src']:
            try:
                image_url =  listing_element.select_one('.gallery-wrapper .gallery-container img')[attr]
            except:
                image_url = ''
            if 'listing' in image_url:
                cover_image=image_url
            
        try:
            listing_data = {
                'listing_id': listing_element.get('data-listing-id'),
                'cover_image': cover_image,
                'url': listing_element.select_one('.listing-description .nav-link')['href'],
                'title': listing_element.select_one('.listing-description h3 a').get('title'),
                'location': listing_element.select_one('.listing-description .listing-location span').text.strip(),
                'price': price,
                'bed': beds,
                'bath': baths,
                'area': area,
                'proximity_mrt': proximity_mrt,
                'prop_details': [li.text.strip() for li in listing_element.select('.listing-description .listing-property-type li')],
                'recency': listing_element.select_one('.listing-description .listing-recency').text.strip(),
                'headline': headline,
                'agent_id': agent_id,
                'agent_name': agent_name,
                'agent_link': agent_link
            }
        except Exception as e: 
            print(f'ERROR LISTING: {i+1}')
            print(traceback.format_exc())
            return pd.DataFrame()
        
        listing_datas.append(listing_data)
        
    page_data = pd.DataFrame(listing_datas)    
    page_data['listing_id'] = page_data['listing_id'].astype(int)
    return page_data

def processAll(htmldir):    
    page_data_ls = []
    for file in tqdm(os.listdir(htmldir)):
        if '.html' in file:
            with open(htmldir+'//'+file, 'r', encoding='utf-8') as file_data:
                html = file_data.read()
                
        page_summary = parseSummary(html)
        #print(page_summary)
        page_details = parseDetail(html)
        #print(page_details)
        # Merge the 2 page_dfs based on ID
        
        #print(page_summary['id'].dtype)
        #print(page_details['listing_id'].dtype)
        page_data_compiled = pd.merge(page_summary, page_details, how = 'left', left_on = 'id', right_on ='listing_id')
       
        # Compile all pages here.
        page_data_ls.append(page_data_compiled)
        
    final_df = pd.concat(page_data_ls)
    return final_df

def cleanTable(compiled_df):
    
    clean_df = pd.DataFrame()
    remap_columns = {
                        'id': 'id',
                        'property_name': 'name',
                        'asking_price': 'price_x',
                        'property_type': 'category',
                        'developer': 'brand',
                        'transaction_type': 'variant',
                        'listing_rank': 'position',
                        'is_turbo': 'dimension23',
                        'district_code': 'districtCode',
                        'region_code': 'regionCode',
                        'beds': 'bedrooms',
                        'baths': 'bathrooms',
                        'area_sqft': 'floorArea',
                        'pg_project_id': 'project',
                        'is_new_project': 'dimension40',
                        'cover_img_url': 'cover_image',
                        'listing_url': 'url',
                        'address': 'location',
                        'proximity_mins':'proximity_mrt',
                        'proximity_m':'proximity_mrt',
                        'proximity_to_mrt':'proximity_mrt',
                        'property_details':'prop_details',
                        'listing_uptime':'recency',
                        'headline':'headline',
                        'agent_id':'agent_id',
                        'agent_name':'agent_name',
                        'agent_link':'agent_link'
                    }
    
    for k,v in remap_columns.items():
        clean_df[k] = compiled_df[v]
        
        
    # Drop columns
    # Clean text
    # Re-type
    return clean_df

if SAVE_FILE_AS != '':
    file_name = SAVE_FILE_AS +'_'
else:
    file_name = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')+ '_'

compiled_df = processAll(PATH_TO_HTMLS)
clean_df = cleanTable(compiled_df)

clean_df.to_csv('data/processed-df//'+ file_name+'df.csv',index=False) # Save compiled df with timestamp to identify the run.
