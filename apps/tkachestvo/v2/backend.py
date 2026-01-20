from apps.tkachestvo.v2.utils import get_last_fullness_date_tkachestvo 
from apps.tkachestvo.v2.utils import get_last_fullness_date_weaving
import os
import traceback
import pylnk3
from python_calamine import CalamineWorkbook
import core.lnk_paths as lnk_paths
import datetime
import core.backend as bknd
#=========================================
file_options = {
        "tkachestvo": {"index": 0, "sheets": {"sheet1": "оборуд"}, "name": "вып отгр (в сокращении)"},
        "weaving": {"index": 1, "sheets": {"sheet1": "База"}, "name": "вып отгр"},
    }


async def default_dict():
    res_dict = {
            "day": {
                "section1": {
                    "section_name": "ВСЕГО станки",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                },
                "section2": {
                    "section_name": "Артикул",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                },
                "section3": {
                    "section_name": "Ассортимент",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                },   
                "section4": {
                    "section_name": "Пневматы/Каретка/Станки 260 см",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                },   
                "section5": {
                    "section_name": "Рапиры/Жаккард/Станки 190 см",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                },  
                
                "section6": {
                    "section_name": "Рапиры/Жаккард/Станки 230 см",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                },  
                
                "section7": {
                    "section_name": "Рапиры/Жаккард/Станки 260 см",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                }, 
                "section8": {
                    "section_name": "Рапиры/Каретка/Станки 190 см",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                }, 
                "section9": {
                    "section_name": "Рапиры/Каретка/Станки 260 см",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                }, 
            },
            "until_day": {
                "section1": {
                    "section_name": "ВСЕГО станки",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                },
                "section2": {
                    "section_name": "Артикул",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                },
                "section3": {
                    "section_name": "Ассортимент",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                },   
                "section4": {
                    "section_name": "Пневматы/Каретка/Станки 260 см",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                },   
                "section5": {
                    "section_name": "Рапиры/Жаккард/Станки 190 см",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                },  
                
                "section6": {
                    "section_name": "Рапиры/Жаккард/Станки 230 см",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                },  
                
                "section7": {
                    "section_name": "Рапиры/Жаккард/Станки 260 см",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                }, 
                "section8": {
                    "section_name": "Рапиры/Каретка/Станки 190 см",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                }, 
                "section9": {
                    "section_name": "Рапиры/Каретка/Станки 260 см",
                    "f_ed": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "",
                    },
                    "percent_fact": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "percent_oper": {
                        "value": 0,
                        "value_total": 0,
                        "k": 0,
                        "unit": "",
                    },
                    "output_pm": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "п.м.",
                    },
                    "output_mil_m_ut": {
                        "value": 0,
                        "value_total": 0,
                        "unit": "млн.м.ут.",
                    },
                }, 
            },
            "fullness": None,
            "fullness_date": None,
            "fullness_descriptions": None,
        }
    return res_dict

async def section_data(res_dict, data, columns, fullness_date, i, section):
    if data[i][columns["дата"]] < fullness_date:
        if data[i][columns["План/ОП"]] == "План":
            # if type(data[i][columns["п_м_м_уточ"]]) in [float, int]:
            #     res_dict["until_day"]["plan_total"] += data[i][columns["п_м_м_уточ"]]
            pass
        
        elif data[i][columns["План/ОП"]] == "ОП":
            # if type(data[i][columns["ОП_п_м_м_уточ"]]) in [float, int]:
            #     res_dict["until_day"]["oper_total"] += data[i][columns["ОП_п_м_м_уточ"]]
            if type(data[i][columns["Кол-во машин ОП"]]) in [float, int]:
                res_dict["until_day"][section]["f_ed"]["value_total"] += data[i][columns["Кол-во машин ОП"]]
                days_set.add(data[i][columns["дата"]])
            if type(data[i][columns["ф_%_работы"]]) in [float, int]:
                res_dict["until_day"][section]["percent_fact"]["value_total"] += data[i][columns["ф_%_работы"]]
                res_dict["until_day"][section]["percent_fact"]["k"] += 1
            if type(data[i][columns["п_%_работы"]]) in [float, int]:
                res_dict["until_day"][section]["percent_oper"]["value_total"] += data[i][columns["п_%_работы"]]
                res_dict["until_day"][section]["percent_oper"]["k"] += 1    
            if type(data[i][columns["ОП_п_м_м_уточ"]]) in [float, int]:
                res_dict["until_day"][section]["output_mil_m_ut"]["value_total"] += data[i][columns["ОП_п_м_м_уточ"]]
            if type(data[i][columns["ОП_пм"]]) in [float, int]:
                res_dict["until_day"][section]["output_pm"]["value_total"] += data[i][columns["ОП_пм"]]
                
    elif data[i][columns["дата"]] == fullness_date:
        if data[i][columns["План/ОП"]] == "План":
            # if type(data[i][columns["п_м_м_уточ"]]) in [float, int]:
            #     res_dict["day"]["plan_total"] += data[i][columns["п_м_м_уточ"]]
            pass
                
        elif data[i][columns["План/ОП"]] == "ОП":
            # if type(data[i][columns["ОП_п_м_м_уточ"]]) in [float, int]:
            #     res_dict["day"]["oper_total"] += data[i][columns["ОП_п_м_м_уточ"]]
            if type(data[i][columns["Кол-во машин ОП"]]) in [float, int]:
                res_dict["day"][section]["f_ed"]["value_total"] += data[i][columns["Кол-во машин ОП"]]
            if type(data[i][columns["ф_%_работы"]]) in [float, int]:
                res_dict["day"][section]["percent_fact"]["value_total"] += data[i][columns["ф_%_работы"]]
                res_dict["day"][section]["percent_fact"]["k"] += 1
            if type(data[i][columns["п_%_работы"]]) in [float, int]:
                res_dict["day"][section]["percent_oper"]["value_total"] += data[i][columns["п_%_работы"]]
                res_dict["day"][section]["percent_oper"]["k"] += 1    
            if type(data[i][columns["ОП_п_м_м_уточ"]]) in [float, int]:
                res_dict["day"][section]["output_mil_m_ut"]["value_total"] += data[i][columns["ОП_п_м_м_уточ"]]
            if type(data[i][columns["ОП_пм"]]) in [float, int]:
                res_dict["day"][section]["output_pm"]["value_total"] += data[i][columns["ОП_пм"]]
    return res_dict

async def calc_average(value, k, round_k = None, percent = False):
    if k == 0:
        return None  # чтобы избежать деления на ноль
    if round_k == None:
        if percent == True:
            return value/k*100
        else: return value/k
    else:
        if percent == True:
            return round(value/k*100, round_k)
        else:
            return round(value/k, round_k)
        

async def calc_percent_plan(plan, oper):
    if plan == 0:
        return None  # чтобы избежать деления на ноль
    return round(oper / plan * 100, 1)
    
    
async def tkachestvo_data(date, lnk_path=lnk_paths.WEAVING):
    global res_dict
    res_dict = await default_dict()
    try:
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
        fact_date = 0
        for i in range(2, len(data)):
            if type(data[i][columns["дата"]]) == datetime.date:
                if data[i][columns["дата"]].month == date.month and data[i][columns["дата"]].year == date.year:
                    if data[i][columns["дата"]] == date:
                        if type(data[i][columns["ф_пм"]]) in [float, int]:
                            fact_date += data[i][columns["ф_пм"]]
                if data[i][columns["дата"]] == (date + datetime.timedelta(days=1)):
                    if fact_date != 0:
                        res_dict["fullness"] = True
                        res_dict["fullness_date"] = date
                    else:
                        res_dict["fullness"] = False
                        break
        if res_dict["fullness"] in [False, None]:
            res_dict["fullness_date"] = await get_last_fullness_date_weaving()
        
        # Get data ===============================================================================
        if lnk_path == lnk_paths.WEAVING:
            src = pylnk3.parse(lnk_path).path[3:].split("\\")
            src = os.path.join("/mnt", "harddrive", *src)
            dst = os.path.join(os.getcwd(), "data")
            await bknd.copy2(src, dst)
            workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
        else:
            lnk_path.seek(0)
            workbook = CalamineWorkbook.from_filelike(lnk_path)
    
        data = workbook.get_sheet_by_name("База").to_python()
        data = await bknd.clear_nan(data)
        fullness_date = res_dict["fullness_date"]
        flag = False
        
        columns = {j:i for i, j in enumerate(data[1])}
        global days_set
        days_set = set()
        fact_date = 0
        
        
        for i in range(2, len(data)):
            if type(data[i][columns["дата"]]) == datetime.date:
                if data[i][columns["дата"]].month == date.month and data[i][columns["дата"]].year == date.year:
                    res_dict = await section_data(res_dict, data, columns, fullness_date, i, "section1")
                    if data[i][columns["Станок_тип"]] == "каретка":
                        if data[i][columns["Станок вид"]] == "пневмат":
                            if data[i][columns["Станок_шир"]] in ["260", 260]:
                                res_dict = await section_data(res_dict, data, columns, fullness_date, i, "section4")
                                
                        elif data[i][columns["Станок вид"]] == "рапира":   
                            if data[i][columns["Станок_шир"]] in ["260", 260]:
                                res_dict = await section_data(res_dict, data, columns, fullness_date, i, "section9")
                            elif data[i][columns["Станок_шир"]] in ["190", 190]:
                                res_dict = await section_data(res_dict, data, columns, fullness_date, i, "section8")
                    elif data[i][columns["Станок_тип"]] == "жаккард":
                        if data[i][columns["Станок вид"]] == "рапира":
                            if data[i][columns["Станок_шир"]] in ["260", 260]:
                                res_dict = await section_data(res_dict, data, columns, fullness_date, i, "section7")
                            elif data[i][columns["Станок_шир"]] in ["230", 230]:
                                res_dict = await section_data(res_dict, data, columns, fullness_date, i, "section6")
                            elif data[i][columns["Станок_шир"]] in ["190", 190]:
                                res_dict = await section_data(res_dict, data, columns, fullness_date, i, "section5")
                    

        for num in range(1, 9): 
            res_dict["day"][f"section{num}"]["f_ed"]["value_total"] = await calc_average(res_dict["day"][f"section{num}"]["f_ed"]["value_total"], 1, 1)
            res_dict["day"][f"section{num}"]["f_ed"]["unit"] = ""
            res_dict["day"][f"section{num}"]["f_ed"]["value"] = (await bknd.format_number(res_dict["day"][f"section{num}"]["f_ed"]["value_total"], res_dict["day"][f"section{num}"]["f_ed"]["unit"], 0)).number
            
            res_dict["until_day"][f"section{num}"]["f_ed"]["value_total"] = await calc_average(res_dict["until_day"][f"section{num}"]["f_ed"]["value_total"], len(days_set), 1)
            res_dict["until_day"][f"section{num}"]["f_ed"]["unit"] = ""
            res_dict["until_day"][f"section{num}"]["f_ed"]["value"] = (await bknd.format_number(res_dict["until_day"][f"section{num}"]["f_ed"]["value_total"], res_dict["until_day"][f"section{num}"]["f_ed"]["unit"], 0)).number
            
            
            res_dict["day"][f"section{num}"]["percent_fact"]["value_total"] = await calc_average(res_dict["day"][f"section{num}"]["percent_fact"]["value_total"], res_dict["day"][f"section{num}"]["percent_fact"]["k"], 1, True)  
            res_dict["day"][f"section{num}"]["percent_fact"]["unit"] = ""
            res_dict["day"][f"section{num}"]["percent_fact"]["value"] = (await bknd.format_number(res_dict["day"][f"section{num}"]["percent_fact"]["value_total"], res_dict["day"][f"section{num}"]["percent_fact"]["unit"], 0)).number
            
            res_dict["until_day"][f"section{num}"]["percent_fact"]["value_total"] = await calc_average(res_dict["until_day"][f"section{num}"]["percent_fact"]["value_total"], res_dict["until_day"][f"section{num}"]["percent_fact"]["k"], 1, True)
            res_dict["until_day"][f"section{num}"]["percent_fact"]["unit"] = ""
            res_dict["until_day"][f"section{num}"]["percent_fact"]["value"] = (await bknd.format_number(res_dict["until_day"][f"section{num}"]["percent_fact"]["value_total"], res_dict["until_day"][f"section{num}"]["percent_fact"]["unit"], 0)).number
            
            
            res_dict["day"][f"section{num}"]["percent_oper"]["value_total"] = await calc_average(res_dict["day"][f"section{num}"]["percent_oper"]["value_total"], res_dict["day"][f"section{num}"]["percent_oper"]["k"], 1, True)  
            res_dict["day"][f"section{num}"]["percent_oper"]["unit"] = ""
            res_dict["day"][f"section{num}"]["percent_oper"]["value"] = (await bknd.format_number(res_dict["day"][f"section{num}"]["percent_oper"]["value_total"], res_dict["day"][f"section{num}"]["percent_oper"]["unit"], 0)).number
            
            res_dict["until_day"][f"section{num}"]["percent_oper"]["value_total"] = await calc_average(res_dict["until_day"][f"section{num}"]["percent_oper"]["value_total"], res_dict["until_day"][f"section{num}"]["percent_oper"]["k"], 1, True)  
            res_dict["until_day"][f"section{num}"]["percent_oper"]["unit"] = ""
            res_dict["until_day"][f"section{num}"]["percent_oper"]["value"] = (await bknd.format_number(res_dict["until_day"][f"section{num}"]["percent_oper"]["value_total"], res_dict["until_day"][f"section{num}"]["percent_oper"]["unit"], 0)).number  
        
        
            res_dict["day"][f"section{num}"]["output_pm"]["value"] = (await bknd.format_number(res_dict["day"][f"section{num}"]["output_pm"]["value_total"], "", 0)).number
            
            res_dict["until_day"][f"section{num}"]["output_pm"]["value"] = (await bknd.format_number(res_dict["until_day"][f"section{num}"]["output_pm"]["value_total"], "", 0)).number  
            
            
            res_dict["day"][f"section{num}"]["output_mil_m_ut"]["value"] = (await bknd.format_number(res_dict["day"][f"section{num}"]["output_mil_m_ut"]["value_total"], "", 0)).number
            
            res_dict["until_day"][f"section{num}"]["output_mil_m_ut"]["value"] = (await bknd.format_number(res_dict["until_day"][f"section{num}"]["output_mil_m_ut"]["value_total"], "", 0)).number  
            # res_dict["day"]["percent_plan"] = await calc_percent_plan(res_dict["day"]["plan_total"], res_dict["day"]["oper_total"])
            # res_dict["until_day"]["percent_plan"] = await calc_percent_plan(res_dict["until_day"]["plan_total"], res_dict["until_day"]["oper_total"])        
            
        # res_dict = {
        #     "tkachestvo": {
        #         "day": {
        #             "f_ed": res_dict["tkachestvo"]["day"]["f_ed"],
        #             "percent_work": res_dict["tkachestvo"]["day"]["percent_work"],
        #         },
        #         "until_day": {
        #             "f_ed": res_dict["tkachestvo"]["until_day"]["f_ed"],
        #             "percent_work": res_dict["tkachestvo"]["until_day"]["percent_work"],
        #         },
        #         "fullness": res_dict["tkachestvo"]["fullness"],
        #         "fullness_date": res_dict["tkachestvo"]["fullness_date"],
        #     },
        #     "weaving": {
        #         "day": {
        #             "percent_plan": res_dict["weaving"]["day"]["percent_plan"],
        #             },
        #         "until_day": {
        #             "percent_plan": res_dict["weaving"]["until_day"]["percent_plan"]
        #             },
        #         "fullness": res_dict["weaving"]["fullness"],
        #         "fullness_date": res_dict["weaving"]["fullness_date"],
        #     },
        #     "fullness": res_dict["fullness"],
        #     "fullness_date": res_dict["fullness_date"],
        #     "fullness_descriptions": res_dict["fullness_descriptions"],
        # }     
        return await bknd.descriptions_creator(res_dict, file_name="Ткачество", date=res_dict["fullness_date"])
    
    except FileNotFoundError:
        return await bknd.descriptions_creator(res_dict, error=Exception, file_name="Вып. отгр. 2024")
    except Exception:
        print(traceback.format_exc())
        return await bknd.descriptions_creator(res_dict, error=Exception, file_name="")