from flask import Flask, render_template
from batch_pipeline import run_pipeline, update_sales_db

app = Flask(__name__)


@app.route('/')
def index():
    plot = run_pipeline()
    update_sales_db()
    return render_template('index.html', plot=plot)


if __name__ == '__main__':
    app.run(debug=True, use_reloader=True)