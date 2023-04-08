import os
import io
import json
import flask
from time import time

from classes.FileMetadata import FileMetadata
from data_sources.Source import Source
from data_sources.GoogleDriveAPI.config import source_name, webhook_route, authenticate
from tasks.GroupDownload import GroupDownloadTask
from logger import log

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload



class GoogleDriveAPISource(Source):

    source_name: str = source_name

    def __init__(self, flask_app: flask.Flask) -> None:
        self.flask_app: flask.Flask = flask_app
        self.create_webhook_endpoint()


    def create_webhook_endpoint(self) -> None:
        """ Creates webhook. """
        self.fetch_start_page_token()
        self.flask_app.add_url_rule(webhook_route, view_func=self.on_change, methods=["POST", "GET"])

    
    def fetch_start_page_token(self) -> None:
        """ Fetches start page token for the webhook. """
        service = authenticate()
        response = service.changes().getStartPageToken().execute()
        self.start_page_token = response.get('startPageToken')


    def on_change(self) -> flask.Response:
        """ HTTP endpoint of changes webhook. """

        request = flask.request

        service = authenticate()

        # TODO check request is it the right one (check token)

        page_token = self.start_page_token
        while True:
            response = service.changes().list(
                pageToken=page_token,
                fields="newStartPageToken, nextPageToken, changes(removed, file(id, name, parents, mimeType, size, md5Checksum, lastModifyingUser(displayName), modifiedTime))"
            ).execute()

            changes = response.get('changes', [])
            changed_files_md = []
            for change in changes:
                file = change.get('file')

                if change.get('removed') or file.get('mimeType') == "application/vnd.google-apps.folder":
                    continue

                changed_files_md.append(file)
                print(change)

            self.download_group(
                changed_files_md
            )

            page_token = response.get('nextPageToken', None)
            if page_token is None:
                self.start_page_token = response.get('newStartPageToken')
                break

        return flask.Response(status=200)


    def download_all_files(self) -> None:
        """ Downloads metadata of all the files and starts downloading its contents. """
        
        service = authenticate()
        page_token = None
        folders_structure = self.get_folders_structure()
        while True:
            log("Starting new batch.", "info", self.source_name)

            response = service.files().list(
                fields="nextPageToken, files(id, name, parents, mimeType, size, md5Checksum, lastModifyingUser(displayName), modifiedTime)",
                q="mimeType != 'application/vnd.google-apps.folder'",
                pageSize=100,
                pageToken=page_token
            ).execute()

            self.download_group(
                response.get('files', []),
                folders_structure
            )

            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break


    def download_group(self, files_metadata: list[dict], folders_structure: dict = None) -> None:
        """
        Initiates GroupDownloadTask.

        Parameters:
        files_metadata (list[dict]): List of file metadata which comes from Google API files/list request.
        folders_structure (dict): All Drive folders as {file_id: path, ...} where `file_id` is folder id, `path` is absolute path to it
        """
        download_task = GroupDownloadTask(self)

        if len(files_metadata) < 1:
            return

        if folders_structure == None:
            folders_structure = self.get_folders_structure()

        for file_meta in files_metadata:
            file_md_obj = FileMetadata()
            mime_type = file_meta.get('mimeType')

            file_md_obj.set_id( file_meta.get('id') )
            
            file_export_extension = self._get_export_extension(mime_type) if mime_type in self._get_exportable_mime_types() else ""
            file_md_obj.set_file_path( folders_structure[file_meta.get('parents')[0]] + '/' + file_meta.get('name') + file_export_extension )

            file_md_obj.set_mime_type( mime_type )
            file_md_obj.set_source( self.source_name )
            file_md_obj.set_modified_time( file_meta.get('modifiedTime') )
            file_md_obj.set_modified_by( file_meta.get('lastModifyingUser').get('displayName') )
            file_md_obj.set_size( file_meta.get('size') )
            file_md_obj.set_md5_checksum( file_meta.get('md5Checksum') )

            download_task.add_file(file_md_obj)

        download_task.start()

    
    def download_file(self, file_metadata: FileMetadata) -> None:
        """ Downloads file from this source. """

        service = authenticate()

        log("Downloading: " + file_metadata.file_path, "debug", self.source_name)

        if file_metadata.mime_type in self._get_exportable_mime_types():
            response = service.files().export_media(
                fileId=file_metadata.id,
                mimeType=self._get_export_mime_type(file_metadata.mime_type)
            )
        else: 
            response = service.files().get_media(
                fileId=file_metadata.id
            )
            
        output_path = 'temp/' + file_metadata.file_path
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        fh = io.FileIO(output_path, 'wb')
        downloader = MediaIoBaseDownload(fh, response)

        done = False
        while done is False:
            status, done = downloader.next_chunk()

        log("Downloaded " + file_metadata.file_path, "info", self.source_name)


    def get_folders_structure(self) -> dict:
        """
        Returns dict of all Drive folders as {file_id: path, ...} where `file_id` is folder id, `path` is absolute path to it.
        """

        page_token = None
        folders_structure = {} # {file_id: path, ...}

        service = authenticate()

        while True:
            response = service.files().list(
                fields="nextPageToken, files(parents, name, id)", 
                q="mimeType = 'application/vnd.google-apps.folder'", 
                pageSize=100, 
                pageToken=page_token
            ).execute()

            parents = {} # {folder_id: parent_id, ...}
            for folder in response.get('files', []):
                parents[folder['id']] = folder['parents']

            names = {} # {folder_id: name, ...}
            for folder in response.get('files', []):
                names[folder['id']] = folder['name']
                
                
            def recursion(folder_id):
                """
                Recursively finds path of given folder going from parent to parent until the root dir.
                """
                if folder_id in folders_structure:
                    return folders_structure[folder_id]

                if folder_id not in parents:
                    file = service.files().get(fileId=folder_id, fields="name").execute()
                    folders_structure[folder_id] = file.get("name")
                    return file.get("name")

                folders_structure[folder_id] = recursion(parents[folder_id][0]) + '/' + names[folder_id]
                return folders_structure[folder_id]


            for folder in response.get('files', []):
                recursion(folder['id'])

            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

        return folders_structure


    def _get_exportable_mime_types(self) -> list[str]:
        return [
            "application/vnd.google-apps.document",
            "application/vnd.google-apps.presentation",
            "application/vnd.google-apps.spreadsheet",
        ]


    def _get_export_mime_type(self, mime_type: str) -> str:
        formats = {
            "application/vnd.google-apps.document": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/vnd.google-apps.presentation": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "application/vnd.google-apps.spreadsheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }

        return formats[mime_type]


    def _get_export_extension(self, mime_type: str) -> str:
        extensions = {
            "application/vnd.google-apps.document": ".docx",
            "application/vnd.google-apps.presentation": ".pptx",
            "application/vnd.google-apps.spreadsheet": ".xlsx",
        }

        return extensions[mime_type]

    def restore_all_files(self, restore_path: str) -> None:
        pass

    def restore_file(self, restore_path: str) -> None:
        pass