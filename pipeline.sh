#!/bin/bash

echo "cd to pg-scrape-auto"
cd ~/Projects/pg-scrape-auto  # Replace with the actual path

echo "activate venv"
source sudo-venv/bin/activate

echo "RUN: botHAR_rpi_fixed.py"
python botHAR_rpi_fixed.py || { echo "Error in botHAR_rpi_fixed.py"; exit 1; }

echo "RUN: scrape_propguru.py"
python scrape_propguru.py || { echo "Error in scrape_propguru.py"; exit 1; }

echo "RUN: process_listings.py"
python process-listings.py || { echo "Error in process_listings.py"; exit 1; }

echo "RUN: clean-table.py"
python clean-table.py || { echo "Error in clean-table.py"; exit 1; }

echo "RUN: generate_snapshot.py"
python generate_snapshot.py || { echo "Error in generate_snapshot.py"; exit 1; }

echo "deactivate venv"
deactivate

echo "pipeline done."