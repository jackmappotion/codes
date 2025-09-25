import os
import gspread

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow


class GoogleSpreadSheetController:
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    def __init__(self, client_secret_json_path, sheet_id, token_path='token.json'):
        self.client_secret_json_path = client_secret_json_path
        self.sheet_id = sheet_id
        self.token_path = token_path
        self._auth()

    def _auth(self):
        creds = None
        # 기존 토큰 재사용
        if os.path.exists(self.token_path):
            creds = Credentials.from_authorized_user_file(self.token_path, self.SCOPES)

        # 토큰 없음/만료 시 갱신 또는 신규 인증
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                # 로컬 브라우저로 사용자 OAuth 동의
                flow = InstalledAppFlow.from_client_secrets_file(self.client_secret_json_path, self.SCOPES)
                creds = flow.run_local_server(port=0)

            # 다음 실행을 위해 저장
            with open(self.token_path, 'w') as token:
                token.write(creds.to_json())
        # gspread 클라이언트 생성
        self.client = gspread.authorize(creds)
        self.spreadsheet = self.client.open_by_key(self.sheet_id)

    def show_worksheets(self):
        worksheets = self.spreadsheet.worksheets()
        return [worksheet.title for worksheet in worksheets]

    def make_worksheet(self, title):
        try:
            self.spreadsheet.add_worksheet(title, 1000, 1000)
        except gspread.exceptions.APIError as e:
            print(e)
        return None

    def get_or_create_worksheet(self, title):
        try:
            sheet = self.spreadsheet.worksheet(title)
            return sheet
        except:
            try:
                sheet = self.spreadsheet.add_worksheet(title, 1000, 1000)
                return sheet
            except gspread.exceptions.APIError as e:
                print(e)

    def del_worksheet(self, title):
        try:
            worksheet = self.spreadsheet.worksheet(title)
            self.spreadsheet.del_worksheet(worksheet)
        except gspread.exceptions.WorksheetNotFound as e:
            print(e)
