import win32com.client
import datetime

regions = ["Europe", "MENA", "Americas", "Asia", "Nordics"]

for region in regions:
    today = datetime.date.today().strftime("%Y%m%d")
    template_filename = f"{region}_Staffing_report.xlsx"
    file_name = str(today) + (" ") + template_filename
    previous_file_name = (datetime.date.today() - datetime.timedelta(1)).strftime("%Y%m%d") + (" ") + template_filename
    xl = win32com.client.DispatchEx("Excel.Application")
    print(str(datetime.datetime.now()) + (" ") + 'Process Start')
    print(f"Processing {file_name}")
    wb = xl.workbooks.open(previous_file_name)
    xl.Visible = False
    wb.RefreshAll()
    # wb.save()
    wb.SaveAs(file_name)
    wb.Close(True)
    print(f"Done {file_name}")
    print(str(datetime.datetime.now()) + (" ") + 'Process End')
