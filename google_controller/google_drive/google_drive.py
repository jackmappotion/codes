import os
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class GoogleDriveController:
    SCOPES = ['https://www.googleapis.com/auth/drive']
    MIME_TYPES = {
        'sheet': 'application/vnd.google-apps.spreadsheet',
        'folder': 'application/vnd.google-apps.folder'
    }

    def __init__(self, google_oauth_credential_json_path):
        self.google_oauth_credential_json_path = google_oauth_credential_json_path
        self._auth()

    def _auth(self):
        creds = None
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', self.SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.google_oauth_credential_json_path, self.SCOPES)
                # 로컬 서버를 열어 사용자가 브라우저에서 직접 인증하게 합니다.
                creds = flow.run_local_server(port=0)

            # 성공적으로 인증했다면, 다음 실행을 위해 정보를 'token.json'에 저장합니다.
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        self.service = build('drive', 'v3', credentials=creds)

    def _find_files(self, name, mime_type, parent_directory_id=None):
        """내부용 메서드로, API 'q' 파라미터를 사용해 효율적으로 파일을 검색합니다."""
        query = f"name = '{name}' and mimeType = '{mime_type}' and trashed = false"
        if parent_directory_id:
            query += f" and '{parent_directory_id}' in parents"

        try:
            results = self.service.files().list(
                q=query,
                fields="files(id, name)"
            ).execute()
            return results.get('files', [])
        except HttpError as error:
            print(f"An error occurred: {error}")
            return []

    def show_files(self, directory_id='root',):
        """
        특정 디렉토리의 모든 파일을 가져옵니다. (페이지네이션 처리)

        Args:
            directory_id (str, optional): 파일 목록을 조회할 폴더의 ID. Defaults to None (최상위).

        Returns:
            list: 파일 정보가 담긴 딕셔너리 리스트
        """
        query = f"'{directory_id}' in parents and trashed = false"
        all_files = []
        page_token = None

        while True:
            params = {
                'q': query,
                'fields': "nextPageToken, files(id, name, shared, mimeType)",
                'pageSize': 1000
            }
            if page_token:
                params['pageToken'] = page_token

            results = self.service.files().list(**params).execute()
            all_files.extend(results.get('files', []))
            page_token = results.get('nextPageToken', None)
            if page_token is None:
                break
        return all_files

    def show_directories(self, parent_directory_id=None):
        """
        모든 폴더(디렉토리) 목록을 가져옵니다. (페이지네이션 처리)

        Args:
            parent_directory_id (str, optional): 특정 폴더 하위의 폴더 목록만 조회할 경우 사용. Defaults to None (최상위).

        Returns:
            list: 폴더 정보가 담긴 딕셔너리 리스트.
        """
        query = f"mimeType = '{self.MIME_TYPES['folder']}' and trashed = false"
        if parent_directory_id:
            query += f" and '{parent_directory_id}' in parents"

        all_directories = []
        page_token = None

        while True:
            params = {
                'q': query,
                'fields': "nextPageToken, files(id, name, shared, mimeType)",
                'pageSize': 1000
            }
            if page_token:
                params['pageToken'] = page_token

            results = self.service.files().list(**params).execute()
            all_directories.extend(results.get('files', []))
            page_token = results.get('nextPageToken', None)
            if page_token is None:
                break
        return all_directories

    def make_directory(self, directory_name, parent_directory_id=None):
        """
        새로운 구글 드라이브 폴더를 생성합니다.

        Args:
            directory_name (str): 생성할 폴더의 이름.
            parent_directory_id (str, optional): 생성될 위치의 상위 폴더 ID. Defaults to None.

        Returns:
            str: 생성된 폴더의 ID.
        """
        file_metadata = {
            'name': directory_name,
            'mimeType': self.MIME_TYPES['folder']
        }
        if parent_directory_id:
            file_metadata['parents'] = [parent_directory_id]

        directory = self.service.files().create(body=file_metadata, fields='id').execute()
        print(f"폴더 '{directory_name}' (ID: {directory['id']})를 생성했습니다.")
        return directory['id']

    def get_or_create_directory_id(self, directory_name, parent_directory_id=None):
        """
        폴더 ID를 찾고, 없으면 새로 생성합니다.

        Args:
            directory_name (str): 찾거나 생성할 폴더의 이름.
            parent_directory_id (str, optional): 검색할 위치의 상위 폴더 ID. Defaults to None.

        Returns:
            str: 찾거나 생성한 폴더의 ID.
        """
        files = self._find_files(directory_name, self.MIME_TYPES['folder'], parent_directory_id)

        if not files:
            print(f"'{directory_name}' 폴더를 찾을 수 없어 새로 생성합니다.")
            return self.make_directory(directory_name, parent_directory_id)
        elif len(files) == 1:
            print(f"기존 폴더 '{directory_name}' (ID: {files[0]['id']})를 찾았습니다.")
            return files[0]['id']
        else:
            raise ValueError(f"'{directory_name}' 이름의 폴더가 여러 개 존재합니다.")

    def make_sheet(self, sheet_name, parent_directory_id='root'):
        file_metadata = {
            'name': sheet_name,
            'mimeType': self.MIME_TYPES['sheet']
        }
        if parent_directory_id:
            file_metadata['parents'] = [parent_directory_id]

        sheet = self.service.files().create(body=file_metadata, fields='id').execute()
        return sheet['id']

    def get_or_create_sheet_id(self, sheet_name, parent_directory_id='root'):
        """
        시트 이름으로 ID를 찾거나, 없으면 새로 생성합니다.

        Args:
            sheet_name (str): 찾거나 생성할 시트의 이름.
            parent_directory_id (str, optional): 검색할 위치의 상위 폴더 ID. Defaults to None.

        Returns:
            str: 찾거나 생성한 시트의 ID.
        """
        files = self._find_files(sheet_name, self.MIME_TYPES['sheet'], parent_directory_id)

        if not files:
            print(f"'{sheet_name}' 시트를 찾을 수 없어 새로 생성합니다.")
            return self.make_sheet(sheet_name, parent_directory_id)
        elif len(files) == 1:
            return files[0]['id']
        else:
            raise ValueError(f"'{sheet_name}' 이름의 시트가 여러 개 존재합니다.")

    def delete_file(self, file_id):
        """
        파일 ID를 사용하여 파일을 영구적으로 삭제합니다.

        Args:
            file_id (str): 삭제할 파일 또는 폴더의 ID.
        """
        try:
            self.service.files().delete(fileId=file_id).execute()
            print(f"파일(ID: {file_id})을 삭제했습니다.")
        except HttpError as error:
            print(f"파일 삭제 중 오류가 발생했습니다: {error}")

    def copy_file(self, file_id, new_name, parent_directory_id):
        """
        특정 Google Drive 파일을 복사합니다.

        Args:
            file_id (str): 복사할 원본 파일 ID
            new_name (str, optional): 복사본의 이름
            directory_id (str, optional): 저장할 폴더 ID

        Returns:
            dict: 복사된 파일의 id와 webViewLink
        """
        body = {
            'name': new_name,
            'parents': [parent_directory_id]
        }

        try:
            copied_file = self.service.files().copy(
                fileId=file_id,
                body=body,
                fields='id, webViewLink'
            ).execute()
            print(f"파일(ID: {file_id})을 복사했습니다. 새 파일 ID: {copied_file.get('id')}")
            return copied_file.get('id')
        except HttpError as error:
            print(f"파일 복사 중 오류가 발생했습니다: {error}")
            return None
