from dash import Dash, html, dcc, page_container
import dash_bootstrap_components as dbc
from dash_iconify import DashIconify
import dash
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()
import os

app = Dash(__name__, 
           use_pages=True,
           external_stylesheets=[dbc.themes.BOOTSTRAP],
           suppress_callback_exceptions=True)

app.title = "📖 Databricks Apps Cookbook 🔍"

def create_sidebar():
    nav_items = []
    
    # Add Introduction link with icon
    nav_items.append(
        dbc.NavLink([
            DashIconify(icon="material-symbols:menu-book", className="me-2"),
            "Introduction"
        ],
        href="/",
        className="sidebar-link text-dark fw-bold",
        active="exact"
        )
    )
    
    # Define category and page order
    sidebar_structure = {
        'Tables': [
            'Read a Delta table',
            'Edit a Delta table',
            'OLTP Database'
        ],
        'Volumes': [
            'Upload a file',
            'Download a file'
        ],
        'AI / ML': [
            'Invoke a model',
            'Run vector search',
            'Connect an MCP server',
            'Invoke a multi-modal LLM'
        ],
        'Business Intelligence': [
            'AI/BI Dashboard',
            'Genie'
        ],
        'Workflows': [
            'Trigger a job',
            'Retrieve job results'
        ],
        'Compute': [
            'Connect'
        ],
        'Authentication': [
            'Get current user'
        ],
        'External services': [
            'External connections',
            'Retrieve a secret'
        ],
        'Settings': [
            'Configuration'
        ]
    }
    
    # Define page-specific icons
    page_icons = {
        'Read a Delta table': 'material-symbols:table-view',
        'Edit a Delta table': 'material-symbols:edit-document',
        'OLTP Database': 'material-symbols:database',
        'Upload a file': 'material-symbols:upload',
        'Download a file': 'material-symbols:download',
        'Invoke a model': 'material-symbols:model-training',
        'Connect an MCP server': 'material-symbols:modeling',
        'Invoke a multi-modal LLM': 'material-symbols:sensors',
        'Run vector search': 'material-symbols:search',
        'AI/BI Dashboard': 'material-symbols:dashboard',
        'Genie': 'material-symbols:chat',
        'Trigger a job': 'material-symbols:play-circle',
        'Retrieve job results': 'material-symbols:list-alt',
        'Connect': 'material-symbols:link',
        'Get current user': 'material-symbols:fingerprint',
        'Retrieve a secret': 'material-symbols:key',
        'External connections': 'material-symbols:link',
        'Configuration': 'material-symbols:settings'
    }
    
    # Group pages by category
    categories = defaultdict(list)
    for page in dash.page_registry.values():
        if page.get("category"):
            categories[page["category"]].append(page)
    
    # Create navigation items in specified order
    for category, page_order in sidebar_structure.items():
        if category in categories:
            category_pages = categories[category]
            # Sort pages according to the specified order
            ordered_pages = sorted(
                category_pages,
                key=lambda x: page_order.index(x["name"]) if x["name"] in page_order else len(page_order)
            )
            
            nav_items.append(
                html.Div([
                    # Category header without icon
                    html.Div(category, className="h6 mt-3 mb-2"),
                    # Pages under the category with icons
                    dbc.Nav([
                        dbc.NavLink([
                            DashIconify(
                                icon=page_icons.get(page["name"], "material-symbols:article"),
                                className="me-2"
                            ),
                            page["name"]
                        ],
                        href=page["relative_path"],
                        active="exact",
                        className="sidebar-link text-dark ps-3"
                        ) for page in ordered_pages
                    ], vertical=True)
                ])
            )
    
    return html.Div(nav_items, className="py-2")

app.layout = html.Div([
    dbc.Container([
        dcc.Location(id='url', refresh=False),
        dcc.Store(id='app-config', storage_type='local', data={
            'host': os.getenv('DATABRICKS_HOST'),
            'token': os.getenv('DATABRICKS_TOKEN'),
            'http_path': os.getenv('DATABRICKS_SQL_HTTP_PATH')
        }),
        dbc.Row([
            # Sidebar
            dbc.Col([
                html.Div([
                    html.Img(src=app.get_asset_url("logo.svg"), className="logo ms-4", style={
                        'width': '30px',
                        'margin-top': '20px'
                    }),
                ], className="sidebar-header"),
                html.Div(create_sidebar(), className="ps-4")
            ], width=2, className="sidebar overflow-auto p-0", style={'maxHeight': '100vh', 'position': 'sticky', 'top': 0}),
            
            dbc.Col([
                page_container
            ], width=10, className="content p-0 px-4 py-4")
        ], className="g-0")
    ], fluid=True, className="vh-100 p-0")
])

if __name__ == '__main__':
    app.run_server(debug=True)
