from pathlib import Path
from pecha_uploader.pipeline import upload_root
from pecha_uploader.config import Destination_url


file_paths = ["resources/json/ཤེར་ཕྱིན་ཡི་གེ་གཅིག་མ།_字般若波羅蜜多經.json"]
for file_path in file_paths:
    upload_root(input_file=Path(file_path), destination_url=Destination_url.STAGING, overwrite=True)
