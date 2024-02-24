@echo off

echo cd to pg-scrape-auto
cd C:\Users\Oliver\Documents\_personal\Coding\git-repos\pg-scrape-auto

echo activate venv
call .min_venv\Scripts\activate 

echo RUN: botHAR.py
python botHAR.py || goto :error

echo RUN: scrape_propguru.py
python scrape_propguru.py || goto :error

echo RUN: process_listings.py
python process-listings.py || goto :error

echo RUN: clean-table.py
python clean-table.py || goto :error

echo RUN: generate_snapshot.py
python generate_snapshot.py || goto :error

echo deactivate venv
deactivate

echo pipeline done.

rem Putting the PC to sleep
%SystemRoot%\System32\rundll32.exe powrprof.dll,SetSuspendState Sleep
