# Batch Processing

### Using the Notebook
To run the notebook, you first need to create and activate the `batch-env` conda environment. After cloning this repo, navigate to the base of the directory in your terminal and execute the following commands:

```conda env create -f environment.yml```

```conda activate batch-data```

Instructions on how to do this can be found [here](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)

### Running the Flask dashboard

The Flask dashboard requires a different environment. Navigate to the `dashboard/` directory and run the following commands:

```python -m venv venv```

This creates a folder to store your environment. To activate the environment:

Mac OS: ```source venv/bin/activate```

Windows: ```venv\Scripts\activate.bat```

Once the environment is activated, install the required packages:

```pip install -r requirements.txt```

Now you can run the Flask app:

```python app.py```