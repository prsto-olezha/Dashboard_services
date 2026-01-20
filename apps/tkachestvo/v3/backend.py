import os
import traceback
import pylnk3
from python_calamine import CalamineWorkbook
import core.lnk_paths as lnk_paths
import datetime
import core.backend as bknd
#=========================================
    
async def tkachestvo_data(date, lnk_path=lnk_paths.WEAVING):
    try:
        lines = []
        # fullness and fullness_date ===============================================================================
        if lnk_path == lnk_paths.WEAVING:
            src = pylnk3.parse(lnk_path).path[3:].split("\\")
            src = os.path.join("/mnt", "harddrive", *src)
            dst = os.path.join(os.getcwd(), "data")
            await bknd.copy2(src, dst)
            workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
        else:
            workbook = CalamineWorkbook.from_filelike(lnk_path)
            
        data = workbook.get_sheet_by_name("База").to_python()
        data = await bknd.clear_nan(data)
        # Tkachestvo fullness
        columns = {j:i for i, j in enumerate(data[1])}
        for i in range(len(data)):
            if type(data[i][columns["дата"]]) == datetime.date:
                if data[i][columns["дата"]].month == date.month and data[i][columns["дата"]].year == date.year:
                    lines.append({
                        "date": date,
                        "index": i,
                        "data": {f"{col}": data[i][columns[col]] for col in columns}})
        return lines
    except Exception:
        print(traceback.print_exc())