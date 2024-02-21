# Introduction

This is a tool that will automatically scrape and clean all listing data from [PropertyGuru](https://www.propertyguru.com.sg/property-for-sale), saving the cleaned tables (snapshots) in your local machine. Each run takes about 30 mins and downloads ~40,000 listings over 2,000+ webpages. The scripts can run in the background once the initial bot bypass is complete (< 1 min). It works with minimal configuration on Windows platform, although can be adapted for Linux machines as well. Your machine will need a display, or a way to emulate a display, as it relies on PyAutoGUI to pass bot detection. 

This data can then be used for various purposes such as monitoring changes over time in listings by categories.

# Setup (Windows)

Clone the repo

    git clone https://github.com/olivesarenice/pg-scrape-auto.git

Then create a `.venv` inside the repo

    python -m venv .venv && .venv\Scripts\activate && pip install -r requirements.txt

## Add local configurations

Before running `pipeline.bat`, prepare these 2 items:

1. Create custom screenshots for your browser inside `pyautogui_imgs/`

    Because each machine has its own resolution and each browser has different themes/ colours/ fonts, you need to create the reference images that pyautogui will use to identify the items that need clicking during bot bypass.

    - `target.png` is the CAPTCHA box:
        ![target](assets/target_example.png)

    - `network.png` and `network2.png` are the Network tabs in the Inspect window, when selected and de-selected (the order doesn't really matter)
        ![network](assets/network_example.png)
        ![network](assets/network2_example.png)

    - `exp_har.png` is the Export HAR button in the Inspect > Network window.
        ![exp_har](assets/exp_har_example.png)

    Open the same windows inside your Chrome browser and screenshot and save the files as necessary. Reference can be found inside `pyautogui_imgs/`.

2. Change settings in `config.json`

        {
            "test_run":0,   #  By default set to 0 which will download all pages. Change to 1 to test the pipeline works - will only download the first 25 pages to test. 
            "path_to_chrome":"C:/Program Files/Google/Chrome/Application/chrome.exe",   # Path to your Chrome executable
            "path_to_imgs":{    # Leave as is if you saved the images to the same folder as the repo, and under the same names.
                "captcha_box":"pyautogui_imgs/target.png", 
                "network_unfocus":"pyautogui_imgs/network.png",
                "network_focus":"pyautogui_imgs/network2.png",
                "export_har":"pyautogui_imgs/exp_har.png"
            },
            "path_to_data_htmls":"data/dl-htmls", # Leave as is
            "path_to_har":"." # Leave as is, but check that when you manually click Export HAR, it is set to save to this repo. See image below:

        }

    You may need to manually go through the actions once so that your browser remembers to save HAR files to this repo:

    1. Go to [https://www.propertyguru.com.sg/property-for-sale/20?] and pass the CAPTCHA
    2. Ctrl-Shift-I to open Inspect, go to Network tab, click on Export HAR..
    3. Change the save directory to this repo `pg-scrap-auto/`
    4. Confirm save and replace to lock in the setting.

    ![config](assets/config_har.png)


## Usage

After setup and config, run `pipeline.bat` and it will automatically scrape and save the clean table into `data/processed-df/cleaned/` with the timestamp as a `.zip` file of ~5MB. 


https://github.com/olivesarenice/pg-scrape-auto/assets/57907886/3b2e77db-34f8-4594-819e-b2b84956525e


# How it works

The `.bat` file runs 3 scripts in sequence prioritising the Scrape First, Parse Later approach:

1. `scrape_propguru.py`

    - Uses pyautogui to control your browser and pass the CAPTCHA. Then saves the headers which contain the verified Cloudflare cookies which will let you send `GET` requests unobstructed for up to 30 mins from the initial CAPTCHA.

    - Using these headers, send the requests in 8 parallel threads to get all 2000+ listing pages. For each webpage, save the HTML into `data/dl-htmls/`. Takes about 15-20 mins depending on Cloudflare throttling. This downloads about ~500MB of webpages.

    - Rather than running the search on all listings with no filter, this scraper is configured to split the searches into these filters, and execute the scrape for each:

            ENABLED_FILTERS = [HDB_1_2RM,
                        HDB_3RM,
                        HDB_4RM,
                        HDB_5RM,
                        HDB_EX,
                        CONDO_APT_EC,
                        LANDED_ALL]

        Previously I just scraped all listings from the no filter url `https://www.propertyguru.com.sg/property-for-sale/`, but I realised PropGuru does not display the HDB Type for HDB listings. Hence, we have to assign their types based on the listings that appear from a known filter query. Filter params are just URL params and are defined in `filter_url_param_config.py`. For condo and landed, the property types are properly displayed, hence no need for further filtering within condo and within landeds. 
        
        You may also configure your own filters by changing the which filters are enabled.

        The fitler used to retrieve each listing is stored in the `scrape_filter_name` column in the final output table.
    

2. `process-listings.py`

    - Loops through each HTML page and extracts the relevant information using BeautifulSoup and pattern matching. Takes about 10 mins and is CPU-bound.

    - Combines the data from the pages into a single dataframe with the relevant data columns

3. `clean-table.py`

    - Final string cleaning, typing, and additional calculations to arrive at a useable table stored in `data/processed-df/cleaned/<TIMESTAMP>.csv.zip`

The scripts automatically delete the intermediate files such as the HTML files and prelim tables. Only the final table is stored per run to save space.

**The entire tool is premised on using headers from the HAR file containing a valid cookie that has been granted by Cloudflare after passing bot detection, hence the need for pyautogui to fool the CAPTCHA**

Note that the URL `https://www.propertyguru.com.sg/property-for-sale/20?` is used to get the HAR file because PropertyGuru only turns on the CAPTCHA page for requests going to pages 20 and above. 

The rest of the script is standard scraping using `requests` and data cleaning using `pandas`.

# Issues

## `requests.get` returns weird symbols and cannot be parsed:

Needed to install `brotli`. This is already included in `requirements.txt`. See this [SO issue](https://stackoverflow.com/questions/49702214/python-requests-response-encoded-in-utf-8-but-cannot-be-decoded)


# Future Developments

The goal is to collect a daily snapshot of all the listings available in PropGuru and use that to trend the market (seller's side) is moving over time.

Motivation: public market data is only available as transactions, which are also usually out of date by at least 2 weeks. If we can view real-time (daily) trends in metrics such as:

- how fast a type of property is selling
- range of prices for a specific project of a specific size (range)

The only people with this information at the moment are the large portals like PropGuru who do not share their listing data in a format that can be easily analysed.

This will help:
1. other sellers to properly price their units
2. other buyers to have an understanding of what a good deal looks like

## System Design

A daily load will be in the range of 40k - 50k listings which amounts to 5MB of compressed storage. Uncompressed, this is about 25MB for 40k rows.

Assuming the average lifetime of a listing is 10 days (based on average uptime of PropGuru listings), this means that on a daily, 1/10 or 10% of the total daily listings will be new (i.e. 5k new listings daily). If we are keeping a running table of all listings since start of scraping, this means the table will gain 5k new listings (rows) a day, or  ~3MB per day. Assuming we keep 3 years of historical data inside the database, this amounts to a maximum table size of 4GB (6M rows) at any given time. 

    D0 = 50k
    D1 = 55k
    D2 = 60k

The final table will look like:

| listing_id | first_listed_date | last_listed_date | listing_details |
| - | - | - | - |
|100001 | 2024-01-01 | NULL | ... |
|100002 | 2024-02-01 | NULL | ... | 
|100003 | 2024-02-03 | 2024-02-05 | ... |

e.g. 100001 and 100002 are still listed as of TODAY. 100003 was taken down in 2 days.

Infrastructure:

1. Local server
    
    - have not decided whether to use RPi w/ screen, old laptop, or PC. 
    - this server will be on 24/7 and runs `pipeline.bat` nightly.
    - resulting .zip files will be saved locally 
    - after, run `snapshot.bat` which will run computations and overwrite the results to the google bigquery db AND to local file.

2. BigQuery

    - handle any further data transformations into multiple tables via triggers

3. micro instance OR RPi to host Flask App for public

    - 

