from flask import Flask, request
import os
import sys
from dotenv import load_dotenv
from threading import Thread

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

    thread = Thread(target=gd.download_all_files)
    thread.start()

    return "<p>Download of all the files started!<br/>Download process status can be found in the logs of the system.</p>"


@application.route("/restore_all")
def restore():

    restore_path = request.args.get('restore_path', None)
    if not restore_path:
        return f"<p>Query parameter restore_path not set. <br/> Example of this endpoint: {request.base_url}?restore_path=MyDisk/BackupFolder</p>"

    global gd
    thread = Thread(target=gd.restore_all_files, kwargs={'restore_path': restore_path})
    thread.start()

    return f"<p>Path: {request.args}</p>"