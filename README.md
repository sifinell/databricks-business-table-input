# Business Table Input

A minimal Dash app to explore and edit Unity Catalog tables from Databricks. It supports unified auth in Databricks Apps and flexible local development via .env, environment variables, or UI-provided credentials.

## Features
- Multi-page UI: Introduction, Configuration, Edit a Delta table
- Configuration stored in browser local storage (`dcc.Store('app-config')`)
- Read, edit inline, stage new rows, and save
- Local development auth via Workspace URL + Access Token or env vars
- Databricks Apps uses unified auth automatically

## Requirements
- Python 3.11+
- Databricks SQL Warehouse and Unity Catalog table
- Permissions for the app/service principal:
  - USE CATALOG on the Catalog
  - USE SCHEMA on the Schema
  - MODIFY on the Unity Catalog table
  - CAN USE on the SQL Warehouse

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optional: create a `.env` in the project root to prefill Configuration and local envs:
```env
DATABRICKS_HOST=https://adb-xxxxxxxxxxxxxxxx.xx.azuredatabricks.net
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXX
DATABRICKS_SQL_HTTP_PATH=/sql/1.0/warehouses/xxxx-xxxx
```

## Run locally
```bash
.venv/bin/python app.py
# If 8050 is busy
PORT=8051 .venv/bin/python app.py
```
Open `http://127.0.0.1:8050` (or your chosen port).

## Configure connection
- Go to the Configuration page
- Set Workspace URL, Access Token, and SQL HTTP Path
- Click Save (stored in your browser)
- Alternatively, rely on `.env` or environment variables

## Using the Tables page
1. Enter fully qualified table name `catalog.schema.table`
2. Load Table to view data
3. Edit cells inline or stage a new row in the form below
4. Click Save Changes to write back
   - Currently performs `INSERT OVERWRITE <table> VALUES (...)`
   - Staged rows are only saved when clicking Save Changes

## Auth behavior
- If host/token provided in Configuration (or env vars) → uses that PAT; queries run as the token owner
- Else → uses `databricks-sdk` unified auth (Databricks Apps or your local profile/CLI/SP)

## Notes
- The app is optimized for small demo tables.
- Keep `.env` out of version control.