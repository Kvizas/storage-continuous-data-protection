import sys, os, getopt

#   Setting imports base path
sys.path.append(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.abspath(__file__)
                )
            )
        )
    )
####################################

import json
import traceback
import time
import math
from dotenv import load_dotenv
    
from logger import log
from data_sources.GoogleDriveAPI.config import source_name, webhook_route, authenticate

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build




def fetch_start_page_token(service):
    response = service.changes().getStartPageToken().execute()
    return response.get('startPageToken')


def stop_watcher(service, resource_id):
    request_body = {
        "id": "watcher",
        "token": os.getenv("GOOGLE_DRIVE_WEBHOOK_TOKEN"),
        "resourceId": resource_id
    }

    try:
        response = service.channels().stop(
            body=request_body
        ).execute()
        log("Watcher halted", "info", source_name)
    except:
        pass


def create_watcher(service):

    request_body = {
        "id": "watcher",
        "token": os.getenv("GOOGLE_DRIVE_WEBHOOK_TOKEN"),
        "type": "webhook",
        "address": os.getenv("ROOT_URL") + webhook_route,
        "expiration": math.trunc(time.time() * 1000 + 86400000) # Expire in 24h
    }

    try:
        response = service.changes().watch(
            body=request_body,
            pageToken=fetch_start_page_token(service)
        ).execute()

    except HttpError:
        log("Watcher creation failed: \n" + traceback.format_exc(), "error", source_name)
        return

    watcher_resource_id = response.get('resourceId')
    log("Watcher successfuly created on " + os.getenv("ROOT_URL") + webhook_route, "info", source_name)



if __name__ == "__main__":

    load_dotenv()

    resource_id = None

    opts, args = getopt.getopt(sys.argv[1:], "r:")
    for opt, arg in opts:
        if opt == "-r":
            resource_id = arg

    if resource_id == None:
        print("Resource ID not provided. Provide it with -r argument.")
        quit()

    service = authenticate()

    stop_watcher(service, resource_id)
    create_watcher(service)