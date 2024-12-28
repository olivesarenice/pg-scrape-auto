@echo off

echo cd to pg-scrape-auto
cd C:\Users\olive\Desktop\Projects\pg-scrape-auto\local

echo activate conda
call conda activate pg-scrape-auto


echo RUN: upload
python src/main.py -run_type full -step upload  || goto :error

echo deactivate conda
conda deactivate

timeout /nobreak /t 10 >nul
