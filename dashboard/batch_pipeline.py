import sqlite3
import json
import plotly
import pandas as pd
import numpy as np
from prophet import Prophet
from datetime import timedelta
import plotly.graph_objects as go


# Extract

# Transform

# Load

# Model and Forecast

# Run Pipeline


def create_plot(sales_forecast):

    fig = go.Figure()
    fig.add_trace(
        go.Line(
            x = sales_forecast['ds'][-30:],
            y = sales_forecast['yhat'][-30:]
        )
    )

    fig.update_layout(
        xaxis_title='Day',
        xaxis_title_standoff=0,
        yaxis_title="Total Daily Sales ($)",
        height=850,
        width=1300,
        margin=dict(t=20, pad=0)
    )

    # Convert Plotly figure to JSON for plotting with js on front end
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    return graphJSON


def update_sales_db():

    '''
    Update sales database with week's worth of new sales data

    Demonstration is weekly batch pipeline. Each iteration should update
    sales database with a week's worth of new daily sales
    '''

    # Sales Database
    sales_con = sqlite3.connect('../data/sales.db')

    # Fake Sales
    new_data_con = sqlite3.connect('../data/new_data.db')

    d = pd.read_sql('SELECT * FROM customer_sales;', sales_con)
    most_recent_date = pd.to_datetime(d['order_date']).max()

    start_idx = most_recent_date + timedelta(days=1)
    end_idx = start_idx + timedelta(days=7)

    dates = [pd.to_datetime(d).strftime("%-m/%-d/%Y") for d in np.arange(start_idx, end_idx, dtype='M8[D]')]

    new_data = pd.read_sql('SELECT * FROM new_customer_sales;', new_data_con)

    new_data.loc[new_data['order_date'].isin(dates)].to_sql('customer_sales', sales_con, index=False, if_exists='append')


