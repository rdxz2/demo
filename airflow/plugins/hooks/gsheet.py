from airflow.hooks.base import BaseHook
from apiclient import discovery
from google.oauth2 import service_account

GSHEET_SCOPES_READ = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/spreadsheets.readonly']


class GSheetHook(BaseHook):
    def __init__(self, sa_filename: str):
        super().__init__()

        creds = service_account.Credentials.from_service_account_file(sa_filename, scopes=GSHEET_SCOPES_READ)
        self.service = discovery.build('sheets', 'v4', credentials=creds)

    def fetch_rows(self, id: str, sheet: str, range: str) -> str:
        rows = self.service.spreadsheets().values().get(spreadsheetId=id, range=f'{sheet}!{range}', valueRenderOption='UNFORMATTED_VALUE').execute()['values']
        return rows
