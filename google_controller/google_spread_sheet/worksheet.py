from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_dataframe import set_with_dataframe


class GSSWorkSheetController:
    def __init__(self, gss_controller, worksheet):
        self.gss_controller = gss_controller
        self.worksheet = worksheet
        self.df = None

    def dataframe2sheet(self, df):
        self.df = df
        set_with_dataframe(self.worksheet, self.df)

    def sheet2dataframe(self):
        self.df = get_as_dataframe(self.worksheet)
        return self.df

    def convert2table(self, title=''):
        if self.df is None:
            self.sheet2dataframe()
        num_rows, num_cols = self.df.shape
        end_row = num_rows + 1
        end_col = num_cols
        requests_body = {
            "requests": [
                {
                    "addTable": {
                        "table": {
                            "name": title,
                            "range": {
                                "sheetId": self.worksheet.id,  # ✨ base_worksheet 객체에서 시트 ID를 가져옵니다.
                                "startRowIndex": 0,            # A1 셀의 행 인덱스
                                "endRowIndex": end_row,        # 데이터의 마지막 행 인덱스 + 1
                                "startColumnIndex": 0,         # A1 셀의 열 인덱스
                                "endColumnIndex": end_col,     # 데이터의 마지막 열 인덱스 + 1
                            },
                        }
                    }
                }
            ]
        }
        self.gss_controller.spreadsheet.batch_update(requests_body)

    def clear_tables(self):
        spreadsheet_meta = self.gss_controller.spreadsheet.fetch_sheet_metadata()
        worksheet_meta = [
            worksheet_meta for worksheet_meta in spreadsheet_meta['sheets']
            if worksheet_meta['properties']['title'] == self.worksheet._properties['title']][0]
        if worksheet_meta.get('tables'):
            requests_body = {
                'requests': [
                    {
                        "deleteTable": {
                            "tableId": table_meta['tableId']
                        }
                    }
                    for table_meta in worksheet_meta['tables']
                ]
            }
            self.gss_controller.spreadsheet.batch_update(requests_body)
            
    def get_table_info(self):
        spreadsheet_meta = self.gss_controller.spreadsheet.fetch_sheet_metadata()
        sheet_info = [sheet for sheet in spreadsheet_meta['sheets'] if sheet['properties']['sheetId'] == self.worksheet.id][0]
        table_info = sheet_info['tables'][0]['range']
        return table_info