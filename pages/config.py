from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import dash
import os

# pages/config.py
dash.register_page(
    __name__,
    path='/config',
    title='Configuration',
    name='Configuration',
    category='Settings',
    icon='settings'
)

def layout():
    return dbc.Container([
        html.H1("Configuration", className="my-4"),
        html.P("Set your Databricks connection details for local development. When deployed in Databricks Apps, leave these blank to use unified auth."),
        dbc.Alert("Values are stored in your browser's local storage only.", color="info", className="mb-3"),
        dbc.Form([
            dbc.Row([
                dbc.Col([
                    dbc.Label("Workspace URL", className="fw-bold mb-2"),
                    dbc.Input(id="cfg-host", type="text", placeholder="https://adb-....azuredatabricks.net", className="mb-3"),
                ], width=12)
            ]),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Access Token", className="fw-bold mb-2"),
                    dbc.Input(id="cfg-token", type="password", placeholder="dapi...", className="mb-3"),
                ], width=12)
            ]),
            dbc.Row([
                dbc.Col([
                    dbc.Label("SQL HTTP Path", className="fw-bold mb-2"),
                    dbc.Input(id="cfg-http-path", type="text", placeholder="/sql/1.0/warehouses/xxxx", className="mb-3"),
                ], width=12)
            ]),
            dbc.Button("Save", id="cfg-save", color="primary", className="me-2"),
            dbc.Button("Load from .env (server)", id="cfg-load-env", color="secondary", outline=True)
        ], className="mt-3"),
        html.Div(id="cfg-status", className="mt-3")
    ], fluid=True, className="py-4")

@callback(
    [Output("cfg-host", "value"), Output("cfg-token", "value"), Output("cfg-http-path", "value")],
    Input("url", "href"),
    State("app-config", "data"),
    prevent_initial_call=False
)
def populate_fields(_, store):
    if not store:
        return None, None, None
    return store.get('host'), store.get('token'), store.get('http_path')

@callback(
    [Output("app-config", "data"), Output("cfg-status", "children")],
    Input("cfg-save", "n_clicks"),
    [State("cfg-host", "value"), State("cfg-token", "value"), State("cfg-http-path", "value"), State("app-config", "data")],
    prevent_initial_call=True
)
def save_config(n, host, token, http_path, store):
    store = store or {}
    store.update({'host': host, 'token': token, 'http_path': http_path})
    return store, dbc.Alert("Configuration saved", color="success")

@callback(
    [Output("cfg-host", "value", allow_duplicate=True),
     Output("cfg-token", "value", allow_duplicate=True),
     Output("cfg-http-path", "value", allow_duplicate=True),
     Output("cfg-status", "children", allow_duplicate=True)],
    Input("cfg-load-env", "n_clicks"),
    prevent_initial_call=True
)
def load_from_env(_):
    return os.getenv('DATABRICKS_HOST'), os.getenv('DATABRICKS_TOKEN'), os.getenv('DATABRICKS_SQL_HTTP_PATH'), dbc.Alert("Loaded values from server .env", color="info")

__all__ = ['layout'] 