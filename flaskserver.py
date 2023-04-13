from flask import Flask
import os
import sys
from dotenv import load_dotenv

from database import setup_tables

from data_sources.GoogleDriveAPI.GoogleDriveAPI import GoogleDriveAPISource

load_dotenv()

sys.path.insert(0, os.path.dirname(__file__))

application = Flask(__name__)

setup_tables()

gd = GoogleDriveAPISource(application)

@application.route("/")
def hello_world():
    return "Hello :)"


@application.route("/download_all")
def download_all():
    global gd
    gd.download_all_files()
    return f"<p>Download of all the files started!<br/>Download process status can be found in the logs of the system.</p>"
