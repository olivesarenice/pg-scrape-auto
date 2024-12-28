@echo off
setlocal

set "target_dir=C:\Users\olive\Desktop\Projects\pg-scrape-auto\local\data\02_zipped"
set "days=7"
cd /d "%target_dir%"
forfiles /p "%target_dir%" /s /m *.* /d -%days% /c "cmd /c del @path"
endlocal
