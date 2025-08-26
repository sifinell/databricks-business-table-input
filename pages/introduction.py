from dash import html, dcc
import dash_bootstrap_components as dbc
import dash

# pages/introduction.py
dash.register_page(
    __name__,
    path='/',
    title='Introduction',
    name='Introduction'
)

def layout():
    return dbc.Container([
        html.H1("Business Table Input", className="my-4"),
        html.P("A minimal Dash app to explore and edit Unity Catalog tables from Databricks, with flexible authentication for local development and Databricks Apps deployment."),
        html.Hr(),
        html.H4("How it works"),
        html.Ul([
            html.Li("Configure your Databricks connection on the Configuration page (or via .env)."),
            html.Li("Use the Tables page to fetch, edit, and write back small tables."),
            html.Li("In Databricks Apps, unified auth is used automatically; locally, use a PAT or env.")
        ]),
        html.H4("Navigation"),
        html.Ul([
            html.Li(dcc.Link("Configuration", href="/config")),
            html.Li(dcc.Link("Edit a Delta table", href="/tables/edit")),
        ]),
    ], fluid=True, className="py-4")

__all__ = ['layout'] 