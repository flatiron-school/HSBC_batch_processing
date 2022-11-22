import sqlite3
import json
import plotly
import pandas as pd
import numpy as np
from prophet import Prophet
from datetime import timedelta
import plotly.graph_objects as go


def extract_sales(db_con):
    
    '''
    Extract sales from sales database and return
    as a pandas DataFrame
    '''
    
    query = f"""
        SELECT * FROM customer_sales;
    """
    
    sales = pd.read_sql(query, db_con)
    
    assert type(sales) == pd.DataFrame
    
    return sales


def transform_sales_data(df):
    
    '''
    Accepts raw data from sales database
    and preps for Prophet model
    '''
    
    # Convert order_date to datetime
    df['order_date'] = pd.to_datetime(df['order_date'])
    
    # Aggregate total daily sales
    daily_sales = df.groupby('order_date').sum()['sales'].reset_index()
    
    # Format columns (date - ds & sales - y)
    daily_sales.columns = ['ds', 'y']
    
    # Ensure columns are correct
    assert len(daily_sales.columns) == 2
    
    return daily_sales


def load_sales_to_warehouse(df, db_con):
    
    '''
    Accepts transformed dataframe and updates
    warehouse with new sales data
    '''
    try:
        # Select most recent data from warehouse
        most_recent_date = pd.read_sql('SELECT MAX(ds) FROM daily_sales;', db_con)['MAX(ds)'].values[0]

        # # Filter df for only new data
        new_daily_sales = df.loc[df['ds'] > most_recent_date]
        
        # # If new data exists, insert into warehouse
        if len(new_daily_sales) > 0:
            new_daily_sales.to_sql('daily_sales', db_con, if_exists='append', index=False)
    except:
        df.to_sql('daily_sales', db_con, if_exists='append', index=False)



def model_and_forecast_sales(db_con):
    
    '''
    Function fits Prophet model on most up to date data
    from data warehouse. Returns daily sales forecast for next 30 days
    '''
    
    # Create Prophet model
    model = Prophet()
    model.add_country_holidays(country_name='US')
    model.add_seasonality(name='monthly', period=30.5, fourier_order=4)
        
    # Load sales data from warehouse in DataFrame
    sales_df = pd.read_sql('SELECT * FROM daily_sales;', db_con)

    # Fit model to sales data
    model.fit(sales_df)
        
    # Make future DataFrame for next 30 days
    future = model.make_future_dataframe(periods=30)
    
    # Forecast
    forecast = model.predict(future)

    assert len(forecast) == len(sales_df) + 30
    
    # Return daily sales forecast
    return forecast


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


def run_pipeline():

    # Sales Database
    sales_con = sqlite3.connect('../data/sales.db')
    # "Data Warehouse"
    warehouse_con = sqlite3.connect('../data/warehouse.db')

    raw_sales = extract_sales(sales_con)

    transformed_sales = transform_sales_data(raw_sales)

    load_sales_to_warehouse(transformed_sales, warehouse_con)

    forecasted_sales = model_and_forecast_sales(warehouse_con)

    plot_data = create_plot(forecasted_sales)

    return plot_data


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


