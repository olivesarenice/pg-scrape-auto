# Usage

Code in the `local/` folder should be run on the **Windows** environment - i.e. through Command Prompt, otherwise, screen-based libraries like `pyautogui` and `webbrowser` will not work!

First, install the environment:

```
conda create -n pg-scrape-auto python=3.12
conda activate pg-scrape-auto
```

Then inside the `local/` folder, run to install all the dependencies:

```
pip install poetry
poetry install 
```

Modify the `config.yaml`. 
- Replace `path_to_chrome` with your executable
- Replace `upload.s3_bucket` with your S3 bucket

Add the credentials of AWS IAM user that has S3 permissions into an `.env` file in `local/src`. The contents should look like:

```
AWS_ACCESS_KEY_ID={###}
AWS_SECRET_ACCESS_KEY={###}
```

Run a test once to setup the correct images and prepare the browser defaults:

1. Open Chrome manually
2. Run `python src/main.py -run_type test -step generate_headers`

Your browser should open an incognito window and go to the site. 
1. Take screenshot of the CAPTCHA box and save it as `target.png` in `local/pyautogui`
2. R-click > Inspect, make sure that you are on the Network tab so that Inspect opens to Network tab by default.
3. Screenshot the export logo and save as `export.png`
4. Manually export the HAR file and save it to the `local/src` directory of this repo. From now on all HAR files will automatically save to this directory.
5. Exit and run `python src/main.py -run_type test -step generate_headers`. The browser should now control itself.

You can run each of the 3 steps in series:

```
python src/main.py -run_type full -step generate_headers
python src/main.py -run_type full -step download_html
python src/main.py -run_type full -step upload
```

or just run the batch file as Administrator.