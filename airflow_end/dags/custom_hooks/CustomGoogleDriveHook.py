"""Hook for Google Drive service"""
from typing import IO, Any, Optional, Sequence, Union
import io
from googleapiclient.discovery import Resource, build
from googleapiclient.http import HttpRequest, MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import os

class CustomGoogleDriveHook(GoogleBaseHook):
    """
    Hook for the Google Drive APIs.
    :param api_version: API version used (for example v3).
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    _conn = None  # type: Optional[Resource]

    def __init__(
        self,
        api_version: str = "v3",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version

    def get_conn(self) -> Any:
        """
        Retrieves the connection to Google Drive.
        :return: Google Drive services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build("drive", self.api_version, http=http_authorized, cache_discovery=False)
        return self._conn

    def _ensure_folders_exists(self, path: str) -> str:
        service = self.get_conn()
        current_parent = "root"
        folders = path.split("/")
        depth = 0
        # First tries to enter directories
        for current_folder in folders:
            self.log.debug("Looking for %s directory with %s parent", current_folder, current_parent)
            conditions = [
                "mimeType = 'application/vnd.google-apps.folder'",
                f"name='{current_folder}'",
                f"'{current_parent}' in parents",
            ]
            result = (
                service.files()
                .list(q=" and ".join(conditions), spaces="drive", fields="files(id, name)")
                .execute(num_retries=self.num_retries)
            )
            files = result.get("files", [])
            if not files:
                self.log.info("Not found %s directory", current_folder)
                # If the directory does not exist, break loops
                break
            depth += 1
            current_parent = files[0].get("id")

        # Check if there are directories to process
        if depth != len(folders):
            # Create missing directories
            for current_folder in folders[depth:]:
                file_metadata = {
                    "name": current_folder,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [current_parent],
                }
                file = (
                    service.files()
                    .create(body=file_metadata, fields="id")
                    .execute(num_retries=self.num_retries)
                )
                self.log.info("Created %s directory", current_folder)

                current_parent = file.get("id")
        # Return the ID of the last directory
        return current_parent

    def get_media_request(self, file_id: str) -> HttpRequest:
        """
        Returns a get_media http request to a Google Drive object.
        :param file_id: The Google Drive file id
        :return: request
        :rtype: HttpRequest
        """
        service = self.get_conn()
        request = service.files().get_media(fileId=file_id)
        return request

    def exists(self, folder_id: str, file_name: str, drive_id: Optional[str] = None):
        """
        Checks to see if a file exists within a Google Drive folder
        :param folder_id: The id of the Google Drive folder in which the file resides
        :param file_name: The name of a file in Google Drive
        :param drive_id: Optional. The id of the shared Google Drive in which the file resides.
        :return: True if the file exists, False otherwise
        :rtype: bool
        """
        return bool(self.get_file_id(folder_id=folder_id, file_name=file_name, drive_id=drive_id))

    def get_file_id(self, folder_id: str, file_name: str, drive_id: Optional[str] = None):
        """
        Returns the file id of a Google Drive file
        :param folder_id: The id of the Google Drive folder in which the file resides
        :param file_name: The name of a file in Google Drive
        :param drive_id: Optional. The id of the shared Google Drive in which the file resides.
        :return: Google Drive file id if the file exists, otherwise None
        :rtype: str if file exists else None
        """
        query = f"name = '{file_name}'"
        if folder_id:
            query += f" and parents in '{folder_id}'"
        service = self.get_conn()
        if drive_id:
            files = (
                service.files()
                .list(
                    q=query,
                    spaces="drive",
                    fields="files(id, mimeType)",
                    orderBy="modifiedTime desc",
                    driveId=drive_id,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    corpora="drive",
                )
                .execute(num_retries=self.num_retries)
            )
        else:
            files = (
                service.files()
                .list(q=query, spaces="drive", fields="files(id, mimeType)", orderBy="modifiedTime desc")
                .execute(num_retries=self.num_retries)
            )
        file_metadata = {}
        if files['files']:
            file_metadata = {"id": files['files'][0]['id'], "mime_type": files['files'][0]['mimeType']}
        return file_metadata

    def upload_file(
        self,
        local_location: str,
        remote_location: str,
        chunk_size: int = 100 * 1024 * 1024,
        resumable: bool = False,
    ) -> str:
        """
        Uploads a file that is available locally to a Google Drive service.
        :param local_location: The path where the file is available.
        :param remote_location: The path where the file will be send
        :param chunk_size: File will be uploaded in chunks of this many bytes. Only
            used if resumable=True. Pass in a value of -1 if the file is to be
            uploaded as a single chunk. Note that Google App Engine has a 5MB limit
            on request size, so you should never set your chunk size larger than 5MB,
            or to -1.
        :param resumable: True if this is a resumable upload. False means upload
            in a single request.
        :return: File ID
        :rtype: str
        """
        service = self.get_conn()
        directory_path, _, file_name = remote_location.rpartition("/")
        if directory_path:
            parent = self._ensure_folders_exists(directory_path)
        else:
            parent = "root"

        file_metadata = {"name": file_name, "parents": [parent]}
        media = MediaFileUpload(local_location, chunksize=chunk_size, resumable=resumable)
        file = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id", supportsAllDrives=True)
            .execute(num_retries=self.num_retries)
        )
        self.log.info("File %s uploaded to gdrive://%s.", local_location, remote_location)
        return file.get("id")

    def download_file(self, file_id: str, file_handle: IO, chunk_size: int = 100 * 1024 * 1024):
        """
        Download a file from Google Drive.
        :param file_id: the id of the file
        :param file_handle: file handle used to write the content to
        :param chunk_size: File will be downloaded in chunks of this many bytes.

        """
        request = self.get_media_request(file_id=file_id)
        self.download_content_from_request(file_handle=file_handle, request=request, chunk_size=chunk_size)

    def get_files_list(self):
        """
        Get filenames from Google Drive.
        :return: file Ids, file Names
        """
        service = self.get_conn()
        results = service.files().list(pageSize=100,
                                       fields="nextPageToken, files(id, name)").execute()

        file_ids = []
        file_names = []

        for i in range(len(results['files'])):
            file_ids.append(results['files'][i]['id'])
            file_names.append(results['files'][i]['name'])

        files_csv_names = [file_names[i] for i in range(len(file_names)) if file_names[i].endswith(".csv")]
        files_csv_ids = [file_ids[file_names.index(files_csv_names[i])] for i in range(len(files_csv_names))]

        return files_csv_ids, files_csv_names

    def download_all_files(self, data_path: str, mode: str = "all"):
        """
        Download all files from Google Drive.
        :rtype: object
        :param data_path: path to save downloaded files
        :param update_mode: {"update","diff"}
            all - load all files to data_path
            diff - load files which doesn't exists in data_path folder
        """

        service = self.get_conn()
        if mode == "all":
            file_ids,file_names = self.get_files_list()
            for i in range(len(file_ids)):
                request = service.files().get_media(fileId=file_ids[i])
                filename = '{0}/{1}'.format(data_path, file_names[i])
                fh = io.FileIO(filename, 'wb')
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
        elif mode == "diff":
            existing_files_in_data_path = os.listdir(data_path)
            file_ids, file_names = self.get_files_list()
            diff = list(set(file_names) - set(existing_files_in_data_path))
            new_file_ids = [file_ids[file_names.index(diff[i])] for i in range(len(diff))]
            for i in range(len(new_file_ids)):
                request = service.files().get_media(fileId=new_file_ids[i])
                filename = '{0}/{1}'.format(data_path, diff[i])
                fh = io.FileIO(filename, 'wb')
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()