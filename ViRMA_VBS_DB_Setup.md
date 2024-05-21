Create Database
================
1. Install PostgreSQL (pgAdmin optional)
2. Create database VBS24: createdb -U postgres VBS24
3. Go to python-loader
4. Load Database Schema: psql -U postgres -f ddl.sql VBS24


How to install gRPC Server
===========================
1. Go to python-loader
2. Enter server
3. Create a Python environment: python -m venv vbs_loader
4. Activate environment: .\vbs_loader\Scripts\activate.bat
5. Install requirements: pip install -r requirements.txt
6. Update database info in app.py: psycopg.connect(...)
7. Run server: python app.py


How to load data
=================
1. Go to python-loader/client
2. Create a Python environment: python -m venv vbs_client
3. Activate environment: .\vbs_client\Scripts\activate.bat
4. Install client: pip install --editable .
You may encounter the following warnings:

WARNING: The script tqdm.exe is installed in 'C:\Users\ok261\AppData\Roaming\Python\Python311\Scripts' which is not on PATH.
  Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.
  WARNING: The script loader.exe is installed in 'C:\Users\ok261\AppData\Roaming\Python\Python311\Scripts' which is not on PATH.
  Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.

4.1 In case of the warning add the specified location to your enviroment PATH

NOTE: After installing the client you may need to reopen the terminal

5. Go to VBS24-Mini
6. Run: loader.exe import vbs_m3_test_ts+m.json
7. Run: loader.exe import vbs_m3_test_ts+h.json


Create Materialized Views
==========================
1. Run views.sql

