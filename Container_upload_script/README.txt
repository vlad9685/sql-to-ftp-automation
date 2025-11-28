INSTRUCTIONS FOR CSV EXPORT SCRIPT

PREREQUISITES:
1. Python 3.10 or higher installed.
2. ODBC Driver 17 (or 18) for SQL Server installed. 
   (Download here: https://go.microsoft.com/fwlink/?linkid=2266675)

SETUP:
1. Unzip this folder.
2. Open a terminal/command prompt in this folder.
3. Install libraries: 
   pip install -r requirements.txt
4. Rename '.env.template' to '.env' and open it with Notepad.
5. Fill in the SQL_PASSWORD and storage connection strings provided separately.

TO RUN:
1. Double click 'run_export.bat' (Create a simple .bat file for them)
   OR run: python generate_csv.py