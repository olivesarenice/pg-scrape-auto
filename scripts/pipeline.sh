#!/bin/bash

echo "cd to pg-scrape-auto"
cd ~/Projects/pg-scrape-auto  # Replace with the actual path

echo "activate venv"
source sudo-venv/bin/activate

echo "RUN: scrape_propguru.py"
python scrape_propguru_rpi.py || { echo "Error in scrape_propguru.py"; exit 1; }

echo "RUN: process_listings.py"
python process_listings.py || { echo "Error in process_listings.py"; exit 1; }

echo "RUN: clean-table.py"
python clean_table.py || { echo "Error in clean-table.py"; exit 1; }

echo "RUN: generate_snapshot.py"
python generate_snapshot.py || { echo "Error in generate_snapshot.py"; exit 1; }

echo "deactivate venv"
deactivate

echo "pipeline done."