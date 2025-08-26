from dash import Dash, html, dcc, callback, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
from databricks import sql
from databricks.sdk.core import Config
import pandas as pd
from functools import lru_cache
import dash
import os
from urllib.parse import urlparse
from dash.dependencies import ALL
from datetime import datetime, date
import numpy as np
import numbers

# pages/tables_edit.py
dash.register_page(
    __name__,
    path='/tables/edit',
    title='Edit Table',
    name='Edit a Delta table',
    category='Tables',
    icon='table'
)

@lru_cache(maxsize=1)
def get_config():
    return Config()

@lru_cache(maxsize=8)
def get_connection(http_path: str, host_override: str | None, token_override: str | None):
    # Priority 1: explicit overrides from UI or store
    if host_override and token_override:
        parsed = urlparse(host_override)
        hostname = parsed.hostname if parsed.hostname else host_override.replace("https://", "").replace("http://", "")
        return sql.connect(
            server_hostname=hostname,
            http_path=http_path,
            access_token=token_override,
        )
    # Priority 2: environment variables for local dev
    env_host = os.getenv("DATABRICKS_HOST")
    env_token = os.getenv("DATABRICKS_TOKEN")
    if env_host and env_token:
        parsed = urlparse(env_host)
        hostname = parsed.hostname if parsed.hostname else env_host.replace("https://", "").replace("http://", "")
        return sql.connect(
            server_hostname=hostname,
            http_path=http_path,
            access_token=env_token,
        )
    # Priority 3: unified auth (Databricks Apps or configured local auth)
    cfg = get_config()
    parsed = urlparse(cfg.host)
    hostname = parsed.hostname if parsed.hostname else cfg.host.replace("https://", "").replace("http://", "")
    return sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )

def get_table_schema(table_name: str, conn):
    # Returns list of dicts: [{'name': ..., 'type': ...}, ...]
    try:
        parts = table_name.split(".")
        if len(parts) != 3:
            return []
        catalog, schema, table = parts
        query = (
            "SELECT column_name, data_type FROM system.information_schema.columns "
            f"WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{table}' "
            "ORDER BY ordinal_position"
        )
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        return [{"name": r[0], "type": r[1]} for r in rows]
    except Exception:
        # Fallback to DESCRIBE TABLE if information_schema not accessible
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"DESCRIBE TABLE {table_name}")
                rows = cursor.fetchall()
            schema = []
            for r in rows:
                col = r[0]
                dtype = r[1]
                if col and not col.startswith("#") and dtype:
                    schema.append({"name": col, "type": dtype})
            return schema
        except Exception:
            return []

def build_new_row_form(schema):
    fields = []
    for col in schema:
        fields.append(
            dbc.Row([
                dbc.Col([
                    dbc.Label(f"{col['name']} ({col['type']})", className="fw-bold mb-2"),
                    dbc.Input(id={"type": "new-field", "name": col["name"]}, type="text", placeholder=f"Enter {col['name']}", className="mb-3",
                              style={
                                  "backgroundColor": "#f8f9fa",
                                  "border": "1px solid #dee2e6",
                                  "boxShadow": "inset 0 1px 2px rgba(0,0,0,0.075)"
                              })
                ], width=12)
            ])
        )
    return html.Div([
        html.H4("Insert new row", className="mt-4 mb-2"),
        html.Div(fields),
        dbc.Button("Add Row", id="add-row-button", color="primary", className="mt-2")
    ])

def parse_value_by_type(value: str, dtype: str):
    if value is None or value == "":
        return None
    dt = dtype.lower()
    try:
        if any(t in dt for t in ["int", "byte", "short"]):
            return int(value)
        if any(t in dt for t in ["decimal", "double", "float", "real"]):
            return float(value)
        if "boolean" in dt:
            return value.lower() in ("true", "1", "yes", "y")
        if "date" in dt and "time" not in dt:
            return str(value)
        if "timestamp" in dt:
            return str(value)
        return str(value)
    except Exception:
        return str(value)

def insert_row(table_name: str, schema, values, conn):
    # values: list aligned to schema order
    coerced = []
    for col, val in zip(schema, values):
        coerced.append(parse_value_by_type(val, col["type"]))
    # Build VALUES clause with repr, handling NULL
    sql_values = ",".join(["NULL" if v is None else repr(v) for v in coerced])
    with conn.cursor() as cursor:
        cursor.execute(f"INSERT INTO {table_name} VALUES ({sql_values})")

def read_table(table_name: str, conn) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name}")
        return cursor.fetchall_arrow().to_pandas()

def insert_overwrite_table(table_name: str, df: pd.DataFrame, conn):
    with conn.cursor() as cursor:
        rows = list(df.itertuples(index=False))
        values = ",".join([f"({','.join(map(repr, row))})" for row in rows])
        cursor.execute(f"INSERT OVERWRITE {table_name} VALUES {values}")

def layout():
    return dbc.Container([
        html.H1("Tables", className="my-4"),
        html.H2("Explore and edit a table", className="mb-3"),
        html.P([
            "Use this page to read, edit, and write back data stored in a small Unity Catalog table with ",
            html.A("Databricks SQL Connector", 
                  href="https://docs.databricks.com/en/dev-tools/python-sql-connector.html",
                  target="_blank",
                  className="text-primary"),
            ". Connection details are managed on the ",
            dcc.Link("Configuration", href="/config", className="text-decoration-none"),
            " page."
        ], className="mb-4"),
        
        dbc.Tabs([
            dbc.Tab(label="Try it", tab_id="try-it", children=[
                dbc.Form([
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Specify a Catalog table name:", className="fw-bold mb-2"),
                            dbc.Input(id="table-name-input", type="text", placeholder="catalog.schema.table", className="mb-3",
                                      style={
                                          "backgroundColor": "#f8f9fa",
                                          "border": "1px solid #dee2e6",
                                          "boxShadow": "inset 0 1px 2px rgba(0,0,0,0.075)"
                                      })
                        ], width=12)
                    ]),
                    dbc.Button("Load Table", id="load-button-edit", color="primary", className="mb-4", size="md")
                ], className="mt-3"),
                dcc.Store(id="schema-store"),
                dbc.Spinner(
                    html.Div(id="table-editor", className="mt-3"),
                    color="primary",
                    type="border",
                    fullscreen=False,
                ),
                html.Div(id="new-row-area", className="mt-3"),
                dbc.Button("Save Changes", id="save-button-edit", color="success", className="mt-3 d-none", size="md"),
                html.Div(id="status-area-edit", className="mt-3")
            ], className="p-3"),
            
            dbc.Tab(label="Requirements", tab_id="requirements", children=[
                dbc.Row([
                    dbc.Col([
                        html.H4("Permissions (app/service principal)", className="mb-3"),
                        html.Ul([
                            dcc.Markdown("**```USE CATALOG```** on the Catalog"),
                            dcc.Markdown("**```USE SCHEMA```** on the Schema"),
                            dcc.Markdown("**```MODIFY```** on the Unity Catalog table"),
                            dcc.Markdown("**```CAN USE```** on the SQL warehouse")
                        ], className="mb-4")
                    ]),
                    dbc.Col([
                        html.H4("Databricks resources", className="mb-3"),
                        html.Ul([
                            html.Li("SQL warehouse"),
                            html.Li("Unity Catalog table")
                        ], className="mb-4")
                    ]),
                    dbc.Col([
                        html.H4("Dependencies", className="mb-3"),
                        html.Ul([
                            dcc.Markdown("* [Databricks SDK](https://pypi.org/project/databricks-sdk/) - `databricks-sdk`"),
                            dcc.Markdown("* [Databricks SQL Connector](https://pypi.org/project/databricks-sql-connector/) - `databricks-sql-connector`"),
                            dcc.Markdown("* [Pandas](https://pypi.org/project/pandas/) - `pandas`"),
                            dcc.Markdown("* [Dash](https://pypi.org/project/dash/) - `dash`")
                        ], className="mb-4")
                    ])
                ])
            ], className="p-3")
        ], id="tabs", active_tab="try-it", className="mb-4")
    ], fluid=True, className="py-4")

@callback(
    [Output("table-editor", "children"),
     Output("save-button-edit", "className"),
     Output("status-area-edit", "children", allow_duplicate=True),
     Output("new-row-area", "children"),
     Output("schema-store", "data")],
    Input("load-button-edit", "n_clicks"),
    [State("table-name-input", "value"), State("app-config", "data")],
    prevent_initial_call=True
)
def load_table_data_edit(n_clicks, table_name, store):
    if not table_name:
        return None, "mt-3 d-none", dbc.Alert("Please provide table name in catalog.schema.table format", color="warning"), None, None
    http_path = (store or {}).get('http_path') if store else None
    host = (store or {}).get('host') if store else None
    token = (store or {}).get('token') if store else None
    if not http_path:
        return None, "mt-3 d-none", dbc.Alert("Missing SQL HTTP Path. Set it in Configuration.", color="warning"), None, None
    try:
        conn = get_connection(http_path, host, token)
        df = read_table(table_name, conn)
        schema = get_table_schema(table_name, conn)
        table = dash_table.DataTable(
            id='editing-table',
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i, 'editable': True} for i in df.columns],
            editable=True,
            row_deletable=True,
            style_table={
                'overflowX': 'auto',
                'minWidth': '100%',
            },
            style_header={
                'backgroundColor': '#f8f9fa',
                'fontWeight': 'bold',
                'border': '1px solid #dee2e6',
                'padding': '12px 15px'
            },
            style_cell={
                'padding': '12px 15px',
                'textAlign': 'left',
                'border': '1px solid #dee2e6',
                'maxWidth': '200px',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis'
            },
            style_data={
                'whiteSpace': 'normal',
                'height': 'auto',
            },
            page_size=10,
            page_action='native',
            sort_action='native',
            sort_mode='multi',
        )
        return table, "mt-3", None, build_new_row_form(schema), schema
    except Exception as e:
        return None, "mt-3 d-none", dbc.Alert(f"Error loading table: {str(e)}", color="danger"), None, None

@callback(
    Output("status-area-edit", "children"),
    Input("save-button-edit", "n_clicks"),
    [State("editing-table", "data"),
     State("table-name-input", "value"),
     State("app-config", "data"),
     State("schema-store", "data")],
    prevent_initial_call=True
)
def save_changes(n_clicks, table_data, table_name, store, schema):
    if not n_clicks:
        return None
    http_path = (store or {}).get('http_path') if store else None
    host = (store or {}).get('host') if store else None
    token = (store or {}).get('token') if store else None
    try:
        conn = get_connection(http_path, host, token)
        df = pd.DataFrame(table_data)
        # Coerce values to match schema to avoid mixed inline types
        schema = schema or []
        ordered_cols = [c.get('name') for c in schema] if schema else df.columns.tolist()
        df = df.reindex(columns=ordered_cols)
        def coerce_series(s, dtype):
            dt = (dtype or '').lower()
            try:
                if any(t in dt for t in ["int", "byte", "short"]):
                    return pd.to_numeric(s, errors='coerce').astype('Int64')
                if any(t in dt for t in ["decimal", "double", "float", "real"]):
                    return pd.to_numeric(s, errors='coerce')
                if "boolean" in dt:
                    return s.map(lambda v: str(v).lower() in ("true","1","yes","y") if v is not None else None)
                # dates/timestamps: leave as strings
                return s
            except Exception:
                return s
        if schema:
            for col in schema:
                name = col.get('name')
                if name in df.columns:
                    df[name] = coerce_series(df[name], col.get('type'))
        # Build SQL literals using schema type per column
        def sql_literal_typed(v, dtype):
            dt = (dtype or '').lower()
            # nulls
            if v is None or pd.isna(v):
                return "NULL"
            # booleans
            if "boolean" in dt:
                if isinstance(v, (bool, np.bool_)):
                    return "TRUE" if bool(v) else "FALSE"
                sv = str(v).lower()
                return "TRUE" if sv in ("true","1","yes","y") else "FALSE"
            # numerics
            if any(t in dt for t in ["int", "byte", "short", "long", "bigint", "tinyint", "smallint", "decimal", "double", "float", "real"]):
                nv = pd.to_numeric(pd.Series([v]), errors='coerce').iloc[0]
                if pd.isna(nv):
                    return "NULL"
                return str(nv)
            # dates/timestamps and strings
            return "'" + str(v).replace("'", "''") + "'"
        row_strings = []
        type_map = {c.get('name'): c.get('type') for c in (schema or [])}
        for _, r in df.iterrows():
            vals = [sql_literal_typed(r[c], type_map.get(c)) for c in df.columns]
            row_strings.append("(" + ",".join(vals) + ")")
        values_sql = ",".join(row_strings)
        with conn.cursor() as cursor:
            cursor.execute(f"INSERT OVERWRITE {table_name} VALUES {values_sql}")
        return dbc.Alert("Changes saved successfully", color="success")
    except Exception as e:
        return dbc.Alert(f"Error saving changes: {str(e)}", color="danger")

@callback(
    [Output("editing-table", "data", allow_duplicate=True),
     Output("status-area-edit", "children", allow_duplicate=True),
     Output("new-row-area", "children", allow_duplicate=True)],
    Input("add-row-button", "n_clicks"),
    [State({'type': 'new-field', 'name': ALL}, 'value'),
     State("schema-store", "data"),
     State("editing-table", "data")],
    prevent_initial_call=True
)
def add_row(n_clicks, new_values, schema, current_data):
    if not n_clicks:
        return dash.no_update, dash.no_update, dash.no_update
    try:
        schema = schema or []
        column_names = [c.get('name') for c in schema]
        # Build new row dict aligned with schema order; fill missing columns with None
        new_row = {}
        for name, val, col in zip(column_names, (new_values or []), schema):
            new_row[name] = parse_value_by_type(val, col.get('type', 'string'))
        # Ensure all columns present
        if current_data and isinstance(current_data, list) and len(current_data) > 0:
            for key in current_data[0].keys():
                new_row.setdefault(key, None)
        updated = (current_data or []) + [new_row]
        return updated, dbc.Alert("Row staged. Click Save Changes to commit.", color="info"), build_new_row_form(schema)
    except Exception as e:
        return dash.no_update, dbc.Alert(f"Error staging row: {str(e)}", color="danger"), dash.no_update

# Make layout available at module level
__all__ = ['layout']
