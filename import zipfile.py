import zipfile

zip_path = "C:/Users/My Pc/Downloads/silver_notebook (1).dbc"

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(r"E:\Data Engineering\NYC TAXI Project"