import os
import traceback
from core.logger import logger
import pylnk3
from python_calamine import CalamineWorkbook
import core.lnk_paths as lnk_paths
import datetime
import subprocess
import shutil
from collections import namedtuple
import core.config as config
import core.descriptions as descriptions

#=======================
from apps.e_com_deviation.v1.utils import get_last_fullness_date as last_fullness_date_ecom
from apps.yarn.v1.utils import get_data_backup, get_last_fullness_date as last_fullness_date_yarn
from apps.sales.v1.utils import get_last_fullness_date as last_fullness_date_sales
from apps.tkachestvo.v1.utils import get_last_fullness_date_tkachestvo 
from apps.tkachestvo.v1.utils import get_last_fullness_date_weaving
# ======================


async def mount():
    command = "echo student | sudo --stdin mount -t cifs -o username=TelegramBot,password=F6CMTX!! //10.0.0.206/OTDEL /mnt/harddrive"
    subprocess.getstatusoutput(command)


async def copy(src, dst):
    command = f'echo student | sudo --stdin cp "{src}" "{dst}"'
    subprocess.getstatusoutput(command)

async def copy2(src, dst):
    try:
        shutil.copy2(src, dst)
        return f"Копирование завершено успешно: {src} -> {dst}"
    except Exception as e:
        logger.error(f"Error processing file copy: {e}")
        return f"Ошибка при копировании: {e}"
        
async def umount():
    command = "echo student | sudo --stdin umount /mnt/harddrive"
    subprocess.getstatusoutput(command)

async def clear_nan(data):
    for i in range(len(data) - 1, 0, -1):
        if all([True if j in ["", " "] else False for j in data[i]]):
            del data[i]
    return data

async def format_number(n: float, unit: str = None, k: int = 2):
    '''
    "": 1
    "тыс": 1e3
    "млн": 1e6
    "млрд": 1e9
    '''
    try:
        NumberUnit = namedtuple("NumberUnit", ["number", "unit"])
        units = {"": 1, "тыс": 1e3, "млн": 1e6, "млрд": 1e9}  # Маппинг единиц
        
        if unit in units:
          n /= units[unit]
          
          formatted_number = f"{n:.{k}f}".rstrip('0').rstrip('.')
          if k == 0:
            return NumberUnit(int(formatted_number), unit)
          else:
            return NumberUnit(float(formatted_number), unit)
        
        index = 0
        unit_list = list(units.keys())
        while abs(n) >= 1000 and index < len(unit_list) - 1:
            n /= 1000
            index += 1
        
        formatted_number = f"{n:.2f}".rstrip('0').rstrip('.')
        return NumberUnit(float(formatted_number), unit_list[index])
    except:
        return NumberUnit(n, None)

async def calc_deviation(plan, oper):
    if plan == 0:
        return None  # чтобы избежать деления на ноль
    return round((oper-plan) / plan * 100, 1)


async def calc_percent_plan(plan, oper):
    if plan == 0:
        return None  # чтобы избежать деления на ноль
    return round(oper / plan * 100, 1)


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

async def calc_profitability(factory_sum, margin_total, round_k):
    if factory_sum == 0:
        return None  # чтобы избежать деления на ноль
    if round_k == None:
        return margin_total/factory_sum*100
    else:
        return round(margin_total/factory_sum*100, round_k)
    
async def calc_redemption_rate(sales_fact, orders_fact, round_k):
    if orders_fact == 0:
        return None  # чтобы избежать деления на ноль
    if round_k == None:
        return sales_fact/orders_fact*100
    else:
        return round(sales_fact/orders_fact*100, round_k)
    
    
async def calc_ecom_share(ecom_revenue, total_revenue, round_k):
    if total_revenue == 0:
        return None  # чтобы избежать деления на ноль
    if round_k == None:
        return ecom_revenue/total_revenue*100
    else:
        return round(ecom_revenue/total_revenue*100, round_k)
    
    
    
async def descriptions_creator(res_dict,
                               error = None,
                               date = None,
                               file_name = None):
    if error == FileNotFoundError:
        res_dict["fullness_descriptions"] = await descriptions.NOFILE_ERROR(file_name=file_name)
    elif error == None:
        res_dict["fullness_descriptions"] = await descriptions.fullness_info(file_name=file_name, date=date)
    return res_dict
# ======================


async def e_com_data(date: datetime.date, lnk_path=lnk_paths.E_COM):
    res_dict = {
        "orders": {
            "month_plan": 0,
            "month_oper": 0,
            "ozon": {},
            "wb": {},
            "lamoda": {},
            "total": {}
            },
        "sales": {
            "month_plan": 0,
            "month_oper": 0,
            "ozon": {},
            "wb": {},
            "lamoda": {},
            "total": {}
            },
        "fullness": 0,
    }
    platforms = {"ozon", "wb", "lamoda", "total"}
    try:
        if lnk_path == lnk_paths.E_COM:
            src = pylnk3.parse(lnk_path).path[3:].split("\\")
            src = os.path.join("/mnt", "harddrive", *src)
            dst = os.path.join(os.getcwd(), "data")
            await copy2(src, dst)
            workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
        else:
            workbook = CalamineWorkbook.from_filelike(lnk_path)

        for sheet_name in workbook.sheet_names:
            data = workbook.get_sheet_by_name(sheet_name).to_python()
            data = await clear_nan(data)
            if sheet_name == "Заказы":
                for platform in platforms:
                    columns = data[3]
                    if platform == "wb":
                        columns = {columns[i]: i for i in [0, 5, 6, 7, 8, 9]}
                    elif platform == "ozon":
                        columns = {columns[i]: i for i in [0, 10, 11, 12, 13, 14]}
                    elif platform == "lamoda":
                        columns = {columns[i]: i for i in [0, 15, 16, 17, 18, 19]}
                    elif platform == "total":
                        columns = {columns[i]: i for i in [0, 1, 2, 3, 4]}

                    for i in range(len(data) - 1, 0, -1):
                        loc_date = data[i][columns["дата"]]
                        
                        if platform == "total":
                            if type(loc_date) == datetime.date:
                                if loc_date.month == date.month and loc_date.year == date.year: 
                                    if data[i][columns["План"]] not in [0, "", " "]:
                                        if type(data[i][columns["План"]]) in [int, float]:
                                            res_dict["orders"]["month_plan"] += data[i][columns["План"]]
                                
                                    if data[i][columns["Опер"]] not in [0, "", " "]:
                                        if type(data[i][columns["Опер"]]) in [int, float]:
                                            res_dict["orders"]["month_oper"] += data[i][columns["Опер"]]
                                
                        if loc_date == date:
                            if data[i][columns["Факт"]] not in [0, "", " "]:

                                res_dict["orders"][platform]["plan_total"] = data[i][
                                    columns["План"]
                                ]
                                res_dict["orders"][platform]["oper_total"] = data[i][
                                    columns["Опер"]
                                ]
                                res_dict["orders"][platform]["fact_total"] = data[i][
                                    columns["Факт"]
                                ]
                                res_dict["orders"][platform]["proc_plan"] = data[i][
                                    columns["%_план"]
                                ]
                            else:
                                res_dict["orders"][platform]["plan_total"] = None
                                res_dict["orders"][platform]["oper_total"] = None
                                res_dict["orders"][platform]["fact_total"] = None
                                res_dict["orders"][platform]["proc_plan"] = None

            elif sheet_name == "Продажи":
                for platform in platforms:
                    columns = data[4]
                    if platform == "wb":
                        columns = {columns[i]: i for i in [0, 5, 6, 7, 8]}
                    elif platform == "ozon":
                        columns = {columns[i]: i for i in [0, 9, 10, 11, 12]}
                    elif platform == "lamoda":
                        columns = {columns[i]: i for i in [0, 13, 14, 15, 16]}
                    elif platform == "total":
                        columns = {columns[i]: i for i in [0, 1, 2, 3, 4]}

                    for i in range(len(data)):
                        loc_date = data[i][columns["дата"]]
                        
                        if platform == "total":
                            if type(loc_date) == datetime.date:
                                if loc_date.month == date.month and loc_date.year == date.year: 
                                    if data[i][columns["План"]] not in [0, "", " "]:
                                        if type(data[i][columns["План"]]) in [int, float]:
                                            res_dict["sales"]["month_plan"] += data[i][columns["План"]]
                                
                                    if data[i][columns["Опер"]] not in [0, "", " "]:
                                        if type(data[i][columns["Опер"]]) in [int, float]:
                                            res_dict["sales"]["month_oper"] += data[i][columns["Опер"]]
                                    
                        if loc_date == date:
                            if data[i][columns["Факт"]] not in [0, "", " "]:
                                res_dict["sales"][platform]["plan_total"] = data[i][
                                    columns["План"]
                                ]
                                res_dict["sales"][platform]["oper_total"] = data[i][
                                    columns["Опер"]
                                ]
                                res_dict["sales"][platform]["fact_total"] = data[i][
                                    columns["Факт"]
                                ]
                                res_dict["sales"][platform]["proc_plan"] = data[i][
                                    columns["%_план"]
                                ]
                            else:
                                res_dict["sales"][platform]["plan_total"] = None
                                res_dict["sales"][platform]["oper_total"] = None
                                res_dict["sales"][platform]["fact_total"] = None
                                res_dict["sales"][platform]["proc_plan"] = None
        if (res_dict["sales"]["total"]["fact_total"] not in [0, "", " ", None]) and (
            res_dict["orders"]["total"]["fact_total"] not in [0, "", " ", None]
        ):
            res_dict["fullness"] = True
        else:
            res_dict["fullness"] = False

    except FileNotFoundError:
        res_dict["fullness"] = None
    
    name_dict = {
        "plan_total": "plan",
        "oper_total": "oper",
        "fact_total": "fact"
        }
    unit = "млн"
    for i in ["orders", "sales"]:
        for platform in platforms:
            for name in name_dict:
                format_data = await format_number(res_dict[i][platform][name], unit)
                res_dict[i][platform][f"{name_dict[name]}"] = format_data.number
            res_dict[i][platform]["unit"] = unit   
    return res_dict

async def e_com_deviation_data(date: datetime.date, lnk_path=[lnk_paths.E_COM, lnk_paths.MARGIN]):
    res_dict = {
        "e_com": {
            "sales": {
                "plan_total": 0,
                "oper_total": 0,
                "deviation": 0
            },
            "orders": {
                "plan_total": 0,
                "oper_total": 0,
                "deviation": 0
            },
            "coef": {
                "day": {
                    "localization_rate": 0,  # Коэффициент локализации
                    "turnover_ratio": 0,    # Коэффициент оборачиваемости
                },
                "days7": {
                    "localization_rate": 0,  # Коэффициент локализации
                    "turnover_ratio": 0,    # Коэффициент оборачиваемости
                },
                "days30": {
                    "localization_rate": 0,  # Коэффициент локализации
                    "turnover_ratio": 0,    # Коэффициент оборачиваемости
                },
            },
            "day": {
                "sales_fact": 0,
                "orders_fact": 0,
                "redemption_rate": 0,  # Коэффициент выкупа
            },
            "days7": {
                "sales_fact": 0,
                "orders_fact": 0,
                "redemption_rate": 0,  # Коэффициент выкупа
            },
            "days30": {
                "sales_fact": 0,
                "orders_fact": 0,
                "redemption_rate": 0,  # Коэффициент выкупа
            },
            "fullness": None,
            "fullness_date": None,
        },
        "margin": {
            "day": {
                "margin_total": 0,
                "factory_sum": 0,
                "margin_total_ecom": 0,
                "factory_sum_ecom": 0,
                "ecom_revenue": 0,
                "total_revenue": 0,
                "profitability": 0,   # Рентабельность
                "profitability_ecom": 0,   # Рентабельность ecom                
                "ecom_share": 0,        # Доля E-com от продаж
            },
            "days7": {
                "margin_total": 0,
                "factory_sum": 0,
                "ecom_revenue": 0,
                "margin_total_ecom": 0,
                "factory_sum_ecom": 0,
                "total_revenue": 0,
                "profitability": 0,   # Рентабельность
                "profitability_ecom": 0,   # Рентабельность ecom
                "ecom_share": 0,        # Доля E-com от продаж
            },
            "days30": {
                "margin_total": 0,
                "factory_sum": 0,
                "margin_total_ecom": 0,
                "factory_sum_ecom": 0,
                "ecom_revenue": 0,
                "total_revenue": 0,
                "profitability": 0,   # Рентабельность
                "profitability_ecom": 0,   # Рентабельность ecom
                "ecom_share": 0,        # Доля E-com от продаж
            },
            "fullness": None,
            "fullness_date": None,
        },
        "fullness": None,
        "fullness_date": None,
        "fullness_descriptions": None,
    }
    file_options = {
        "e_com": {"index": 0, "sheets": {"orders": "Заказы", "sales": "Продажи", "coef": "Коэф"}, "name": "Ежедневка E-COM"},
        "margin": {"index": 1, "sheets": {"sheet1": "база"}, "name": "Маржа"}
    }
    try:
        yesterday = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).date()
        days7_array = [(date - datetime.timedelta(days=i)) for i in range(6, -1, -1)]
        days30_array = [(date - datetime.timedelta(days=i)) for i in range(29, -1, -1)]
        #fullness and fullness_date ===================================================================================
        for active_file, options in file_options.items():
            if lnk_path == [lnk_paths.E_COM, lnk_paths.MARGIN]:
                src = pylnk3.parse(lnk_path[options["index"]]).path[3:].split("\\")
                src = os.path.join("/mnt", "harddrive", *src)
                dst = os.path.join(os.getcwd(), "data")
                await copy2(src, dst)
                workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
            else:
                workbook = CalamineWorkbook.from_filelike(lnk_path[options["index"]])
            
            for sheet in options["sheets"]:
                data = workbook.get_sheet_by_name(options["sheets"][sheet]).to_python()
                data = await clear_nan(data)
                if active_file == "e_com" and options["sheets"][sheet] == "Заказы":
                    for i in range(len(data)):
                        loc_date = data[i][0]
                        if type(loc_date) == datetime.date:
                            if loc_date.month == date.month and loc_date.year == date.year:
                                if loc_date == date:
                                    if data[i][3] not in [0, " ", ""]:
                                        res_dict[active_file]["fullness"] = True
                                        res_dict[active_file]["fullness_date"] = date
                                    else: 
                                        res_dict[active_file]["fullness"] = False
                                        res_dict[active_file]["fullness_date"] = await last_fullness_date_ecom()
                                    break
                if active_file == "margin" and options["sheets"][sheet] == "база":
                    dates_set = set()
                    for i in range(len(data)):
                        if type(data[i][3]) == datetime.date:
                            dates_set.add(data[i][3])
                    res_dict[active_file]["fullness_date"] = max(dates_set)
                    if res_dict[active_file]["fullness_date"] >= date:
                        res_dict[active_file]["fullness"] = True
                    else: res_dict[active_file]["fullness"] = False
                    
        res_dict["fullness_date"] = min([res_dict[active_file]["fullness_date"] for active_file in file_options])
        res_dict["fullness"] = all([res_dict[active_file]["fullness"] for active_file in file_options])
        
        
        # Get data from files ===================================================================================
        for active_file, options in file_options.items():
            if lnk_path == [lnk_paths.E_COM, lnk_paths.MARGIN]:
                src = pylnk3.parse(lnk_path[options["index"]]).path[3:].split("\\")
                src = os.path.join("/mnt", "harddrive", *src)
                dst = os.path.join(os.getcwd(), "data")
                await copy2(src, dst)
                workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
            else:
                lnk_path[options["index"]].seek(0)
                workbook = CalamineWorkbook.from_filelike(lnk_path[options["index"]])
                
            for sheet in options["sheets"]:
                data = workbook.get_sheet_by_name(options["sheets"][sheet]).to_python()
                data = await clear_nan(data)
                if active_file == "e_com":
                    if options["sheets"][sheet] == "Заказы":
                        for i in range(len(data)):
                            loc_date = data[i][0]
                            if type(loc_date) == datetime.date:
                                if loc_date.month == date.month and loc_date.year == date.year:
                                    if type(data[i][1]) in [float, int]:
                                        res_dict[active_file][sheet]["plan_total"] += data[i][1]
                                    if type(data[i][2]) in [float, int]:
                                        res_dict[active_file][sheet]["oper_total"] += data[i][2]
                                if loc_date in days30_array:
                                    if type(data[i][3]) in [float, int]:
                                        res_dict[active_file]["days30"]["orders_fact"] += data[i][3]
                                    if loc_date == date:
                                        if type(data[i][3]) in [float, int]:
                                            res_dict[active_file]["day"]["orders_fact"] += data[i][3]
                                        break
                                if loc_date in days7_array:
                                    if type(data[i][3]) in [float, int]:
                                        res_dict[active_file]["days7"]["orders_fact"] += data[i][3]
                                        
                        res_dict[active_file][sheet]["deviation"] = await calc_deviation(res_dict[active_file][sheet]["plan_total"],
                                                               res_dict[active_file][sheet]["oper_total"])
                    #================================================  
                    if options["sheets"][sheet] == "Продажи":
                        for i in range(len(data)):
                            loc_date = data[i][0]
                            if type(loc_date) == datetime.date:
                                if loc_date.month == date.month and loc_date.year == date.year:
                                    if type(data[i][1]) in [float, int]:
                                        res_dict[active_file][sheet]["plan_total"] += data[i][1]
                                    if type(data[i][2]) in [float, int]:
                                        res_dict[active_file][sheet]["oper_total"] += data[i][2]
                                if loc_date in days30_array:
                                    if type(data[i][3]) in [float, int]:
                                        res_dict[active_file]["days30"]["sales_fact"] += data[i][3]
                                    if loc_date == date:
                                        if type(data[i][3]) in [float, int]:
                                            res_dict[active_file]["day"]["sales_fact"] += data[i][3]
                                        break
                                if loc_date in days7_array:
                                    if type(data[i][3]) in [float, int]:
                                        res_dict[active_file]["days7"]["sales_fact"] += data[i][3]
                        res_dict[active_file][sheet]["deviation"] = await calc_deviation(res_dict[active_file][sheet]["plan_total"],
                                                               res_dict[active_file][sheet]["oper_total"])
                    #================================================
                    if options["sheets"][sheet] == "Коэф":
                        k_30 = 0
                        k_7 = 0
                        for i in range(len(data)):
                            loc_date = data[i][0]
                            if type(loc_date) == datetime.date:
                                if loc_date in days30_array:
                                    if data[i][3] not in ["", " ", 0] :
                                        if type(data[i][3]) in [float, int] and type(data[i][6]) in [float, int]:
                                            res_dict[active_file][sheet]["days30"]["localization_rate"] += data[i][3]
                                            res_dict[active_file][sheet]["days30"]["turnover_ratio"] += data[i][6]
                                            k_30+=1
                                if loc_date == date:
                                    if type(data[i][3]) in [float, int] and type(data[i][6]) in [float, int]:
                                        res_dict[active_file][sheet]["day"]["localization_rate"] += data[i][3]
                                        res_dict[active_file][sheet]["day"]["turnover_ratio"] += data[i][6]
                                if loc_date in days7_array:
                                    if data[i][3] not in ["", " ", 0]:
                                        if type(data[i][3]) in [float, int] and type(data[i][6]) in [float, int]:
                                            res_dict[active_file][sheet]["days7"]["localization_rate"] += data[i][3]
                                            res_dict[active_file][sheet]["days7"]["turnover_ratio"] += data[i][6]
                                            k_7+=1
                        res_dict[active_file][sheet]["days30"]["localization_rate"] = await calc_average(res_dict[active_file][sheet]["days30"]["localization_rate"], k_30, 0, True)
                        res_dict[active_file][sheet]["days7"]["localization_rate"] = await calc_average(res_dict[active_file][sheet]["days7"]["localization_rate"], k_7, 0, True)
                        res_dict[active_file][sheet]["day"]["localization_rate"] = await calc_average(res_dict[active_file][sheet]["day"]["localization_rate"], 1, 0, True)
                        res_dict[active_file][sheet]["days30"]["turnover_ratio"] = await calc_average(res_dict[active_file][sheet]["days30"]["turnover_ratio"], k_30, 0)
                        res_dict[active_file][sheet]["days7"]["turnover_ratio"] = await calc_average(res_dict[active_file][sheet]["days7"]["turnover_ratio"], k_7, 0)
                        res_dict[active_file][sheet]["day"]["turnover_ratio"] = await calc_average(res_dict[active_file][sheet]["day"]["turnover_ratio"], 1, 0)      
                #=======================================================================================================
                if active_file == "margin":
                    if options["sheets"][sheet] == "база":
                        columns = {j:i for i, j in enumerate(data[1])}
                        for i in range(len(data)):
                            loc_date = data[i][columns["По дням"]]
                            if type(loc_date) == datetime.date:
                                
                                if loc_date in days30_array:
                                    if type(data[i][columns["Маржа, руб"]]) in [float, int]:
                                        res_dict[active_file]["days30"]["margin_total"] += data[i][columns["Маржа, руб"]]
                                        if data[i][columns["Направление"]] == "e-com":
                                            res_dict[active_file]["days30"]["margin_total_ecom"] += data[i][columns["Маржа, руб"]]
                                    if type(data[i][columns["Сумма по цене фабрики"]]) in [float, int]:
                                        res_dict[active_file]["days30"]["factory_sum"] += data[i][columns["Сумма по цене фабрики"]]
                                        if data[i][columns["Направление"]] == "e-com":
                                            res_dict[active_file]["days30"]["factory_sum_ecom"] += data[i][columns["Сумма по цене фабрики"]]
                                    
                                    if type(data[i][columns["Выручка, с НДС"]]) in [float, int]:
                                        res_dict[active_file]["days30"]["total_revenue"] += data[i][columns["Выручка, с НДС"]]
                                        if data[i][columns["Направление"]] == "e-com":
                                            res_dict[active_file]["days30"]["ecom_revenue"] += data[i][columns["Выручка, с НДС"]]
                                            
                                            
                                if loc_date == date:
                                    if type(data[i][columns["Маржа, руб"]]) in [float, int]:
                                        res_dict[active_file]["day"]["margin_total"] += data[i][columns["Маржа, руб"]]
                                        if data[i][columns["Направление"]] == "e-com":
                                            res_dict[active_file]["day"]["margin_total_ecom"] += data[i][columns["Маржа, руб"]]
                                            
                                    if type(data[i][columns["Сумма по цене фабрики"]]) in [float, int]:
                                        res_dict[active_file]["day"]["factory_sum"] += data[i][columns["Сумма по цене фабрики"]]
                                        if data[i][columns["Направление"]] == "e-com":
                                            res_dict[active_file]["day"]["factory_sum_ecom"] += data[i][columns["Сумма по цене фабрики"]]
                                            
                                    if type(data[i][columns["Выручка, с НДС"]]) in [float, int]:
                                        res_dict[active_file]["day"]["total_revenue"] += data[i][columns["Выручка, с НДС"]]
                                        if data[i][columns["Направление"]] == "e-com":
                                            res_dict[active_file]["day"]["ecom_revenue"] += data[i][columns["Выручка, с НДС"]]
                                     
                                     
                                if loc_date in days7_array:
                                    if type(data[i][columns["Маржа, руб"]]) in [float, int]:
                                        res_dict[active_file]["days7"]["margin_total"] += data[i][columns["Маржа, руб"]]
                                        if data[i][columns["Направление"]] == "e-com":
                                            res_dict[active_file]["days7"]["margin_total_ecom"] += data[i][columns["Маржа, руб"]]
                                            
                                    if type(data[i][columns["Сумма по цене фабрики"]]) in [float, int]:
                                        res_dict[active_file]["days7"]["factory_sum"] += data[i][columns["Сумма по цене фабрики"]]
                                        if data[i][columns["Направление"]] == "e-com":
                                            res_dict[active_file]["days7"]["factory_sum_ecom"] += data[i][columns["Сумма по цене фабрики"]]
                                        
                                    if type(data[i][columns["Выручка, с НДС"]]) in [float, int]:
                                        res_dict[active_file]["days7"]["total_revenue"] += data[i][columns["Выручка, с НДС"]]
                                        if data[i][columns["Направление"]] == "e-com":
                                            res_dict[active_file]["days7"]["ecom_revenue"] += data[i][columns["Выручка, с НДС"]] 
                                                
                        res_dict[active_file]["days7"]["profitability"] = await calc_profitability(res_dict[active_file]["days7"]["factory_sum"], res_dict[active_file]["days7"]["margin_total"], 0)
                        res_dict[active_file]["days30"]["profitability"] = await calc_profitability(res_dict[active_file]["days30"]["factory_sum"], res_dict[active_file]["days30"]["margin_total"], 0)
                        res_dict[active_file]["day"]["profitability"] = await calc_profitability(res_dict[active_file]["day"]["factory_sum"], res_dict[active_file]["day"]["margin_total"], 0)
                        
                        res_dict[active_file]["days7"]["profitability_ecom"] = await calc_profitability(res_dict[active_file]["days7"]["factory_sum_ecom"], res_dict[active_file]["days7"]["margin_total_ecom"], 0)
                        res_dict[active_file]["days30"]["profitability_ecom"] = await calc_profitability(res_dict[active_file]["days30"]["factory_sum_ecom"], res_dict[active_file]["days30"]["margin_total_ecom"], 0)
                        res_dict[active_file]["day"]["profitability_ecom"] = await calc_profitability(res_dict[active_file]["day"]["factory_sum_ecom"], res_dict[active_file]["day"]["margin_total_ecom"], 0)
                        
                        res_dict[active_file]["days7"]['ecom_share'] = await calc_ecom_share(res_dict[active_file]["days7"]["ecom_revenue"], res_dict[active_file]["days7"]["total_revenue"], 0)
                        res_dict[active_file]["days30"]['ecom_share'] = await calc_ecom_share(res_dict[active_file]["days30"]["ecom_revenue"], res_dict[active_file]["days30"]["total_revenue"], 0)
                        res_dict[active_file]["day"]['ecom_share'] = await calc_ecom_share(res_dict[active_file]["day"]["ecom_revenue"], res_dict[active_file]["day"]["total_revenue"], 0)

        res_dict["e_com"]["days7"]["redemption_rate"] = await calc_redemption_rate(res_dict["e_com"]["days7"]["sales_fact"], res_dict["e_com"]["days7"]["orders_fact"], 0)
        res_dict["e_com"]["days30"]["redemption_rate"] = await calc_redemption_rate(res_dict["e_com"]["days30"]["sales_fact"], res_dict["e_com"]["days30"]["orders_fact"], 0)
        res_dict["e_com"]["day"]["redemption_rate"] = await calc_redemption_rate(res_dict["e_com"]["day"]["sales_fact"], res_dict["e_com"]["day"]["orders_fact"], 0)
        res_dict = await e_com_deviation_res_gen(res_dict)
        return await descriptions_creator(res_dict, date=res_dict["fullness_date"], file_name="E-com")
    except Exception:
        print(traceback.format_exc())
        res_dict = await e_com_deviation_res_gen(res_dict)
        return await descriptions_creator(res_dict, error=Exception, file_name=options["name"])


async def e_com_deviation_res_gen(res_dict):
    res_dict = {
        "sales": {
            "plan_total": res_dict["e_com"]["sales"]["plan_total"],
            "oper_total": res_dict["e_com"]["sales"]["oper_total"],
            "deviation": res_dict["e_com"]["sales"]["deviation"],
        },
        "orders": {
            "plan_total": res_dict["e_com"]["orders"]["plan_total"],
            "oper_total": res_dict["e_com"]["orders"]["oper_total"],
            "deviation": res_dict["e_com"]["orders"]["deviation"],
        },
        "table": {
            "day": {
                "redemption_rate": res_dict["e_com"]["day"]["redemption_rate"],  # Коэффициент выкупа
                "profitability": res_dict["margin"]["day"]["profitability"],   # Рентабельность
                "profitability_ecom": res_dict["margin"]["day"]["profitability_ecom"],   # Рентабельность ecom
                "localization_rate": res_dict["e_com"]["coef"]["day"]["localization_rate"],  # Коэффициент локализации
                "turnover_ratio": res_dict["e_com"]["coef"]["day"]["turnover_ratio"],    # Коэффициент оборачиваемости
                "ecom_share": res_dict["margin"]["day"]['ecom_share'],        # Доля E-com от продаж
                },
            "days7": {
                "redemption_rate": res_dict["e_com"]["days7"]["redemption_rate"],  # Коэффициент выкупа
                "profitability": res_dict["margin"]["days7"]["profitability"],   # Рентабельность
                "profitability_ecom": res_dict["margin"]["days7"]["profitability_ecom"],   # Рентабельность ecom
                "localization_rate": res_dict["e_com"]["coef"]["days7"]["localization_rate"],  # Коэффициент локализации
                "turnover_ratio": res_dict["e_com"]["coef"]["days7"]["turnover_ratio"],    # Коэффициент оборачиваемости
                "ecom_share": res_dict["margin"]["days7"]['ecom_share'],        # Доля E-com от продаж
                },
            "days30": {
                "redemption_rate": res_dict["e_com"]["days30"]["redemption_rate"],  # Коэффициент выкупа
                "profitability": res_dict["margin"]["days30"]["profitability"],   # Рентабельность
                "profitability_ecom": res_dict["margin"]["days30"]["profitability_ecom"],   # Рентабельность ecom
                "localization_rate": res_dict["e_com"]["coef"]["days30"]["localization_rate"],  # Коэффициент локализации
                "turnover_ratio": res_dict["e_com"]["coef"]["days30"]["turnover_ratio"],    # Коэффициент оборачиваемости
                "ecom_share": res_dict["margin"]["days30"]['ecom_share'],        # Доля E-com от продаж
            },
        },
        "fullness": res_dict["fullness"],
        "fullness_date": res_dict["fullness_date"],
        "fullness_descriptions": None,
    }
    
    unit = "млн"
    name_dict = {
        "plan_total": "plan",
        "oper_total": "oper",
    }
    for i in ["orders", "sales", "table"]:
        if i == "table":
            for days in ["days7", "days30"]:
                table_dict = {
                    "localization_rate": "localization_rate",
                    "turnover_ratio": "turnover_ratio"
                } 
                for name in table_dict:
                    format_data = await format_number(res_dict[i][days][name], "", 2)
                    res_dict[i][days][f"{table_dict[name]}"] = format_data.number
        else:
            for name in name_dict:
                if name in ["deviation"]:
                    continue
                format_data = await format_number(res_dict[i][name], unit, 1)
                res_dict[i][f"{name_dict[name]}"] = format_data.number
                res_dict[i]["unit"] = unit 
    return res_dict


async def weaving_data(date, lnk_path=lnk_paths.WEAVING):
    res_dict = {
        "fullness": None,
        "pnevmat": {"plan_total": 0, "oper_total": 0, "fact_total": 0, "proc_plan": 0},
        "rapira": {"plan_total": 0, "oper_total": 0, "fact_total": 0, "proc_plan": 0},
        "total": {"plan_total": 0, "oper_total": 0, "fact_total": 0, "proc_plan": 0},
    }

    k_dict = {
        "pnevmat": {"plan_total": 0, "oper_total": 0, "fact_total": 0, "proc_plan": 0},
        "rapira": {"plan_total": 0, "oper_total": 0, "fact_total": 0, "proc_plan": 0},
        "total": {"plan_total": 0, "oper_total": 0, "fact_total": 0, "proc_plan": 0},
    }

    if lnk_path == lnk_paths.WEAVING:
        src = pylnk3.parse(lnk_path).path[3:].split("\\")
        src = os.path.join("/mnt", "harddrive", *src)
        dst = os.path.join(os.getcwd(), "data")
        await copy2(src, dst)
        workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
    else:
        workbook = CalamineWorkbook.from_filelike(lnk_path)
        
    data = workbook.get_sheet_by_name("База").to_python()
    data = await clear_nan(data)
    for i in range(len(data)):
        loc_date = data[i][0]
        if loc_date == date:
            if data[i][12] == "рапира":
                type_machine = "rapira"
            elif data[i][12] == "пневмат":
                type_machine = "pnevmat"

            if data[i][40] == "План":
                if type(data[i][33]) in [float, int]:
                    res_dict[type_machine]["plan_total"] += data[i][33]
                    k_dict[type_machine]["plan_total"] += 1

                    res_dict["total"]["plan_total"] += data[i][33]
                    k_dict["total"]["plan_total"] += 1
                
            elif data[i][40] == "ОП":
                if type(data[i][36]) in [float, int]:
                    if data[i][36] not in [0, "", " "]:
                        res_dict[type_machine]["oper_total"] += data[i][36]
                        k_dict[type_machine]["oper_total"] += 1

                        res_dict["total"]["oper_total"] += data[i][36]
                        k_dict["total"]["oper_total"] += 1
                
                    if data[i][42] not in [0, "", " "]:
                        if type(data[i][42]) in [float, int]:
                            res_dict[type_machine]["fact_total"] += data[i][42]
                            k_dict[type_machine]["fact_total"] += 1
                            res_dict["total"]["fact_total"] += data[i][42]
                            k_dict["total"]["fact_total"] += 1
                        if type(data[i][44]) in [float, int]:
                            res_dict[type_machine]["proc_plan"] += data[i][44]
                            k_dict[type_machine]["proc_plan"] += 1
                            res_dict["total"]["proc_plan"] += data[i][44]
                            k_dict["total"]["proc_plan"] += 1

    for type_machine in res_dict:
        try:
            res_dict[type_machine]["proc_plan"] = res_dict[type_machine]["fact_total"] / (
                res_dict[type_machine]["plan_total"] / 100
            )
        except:
            pass
        
    try:
        res_dict["total"]["proc_plan"] = res_dict["total"]["fact_total"] / (
            res_dict["total"]["plan_total"] / 100
        )
    except:
        pass
    
    if res_dict["total"]["fact_total"] != 0:
        res_dict["fullness"] = True
    else: res_dict["fullness"] = False
    
    name_dict = {
        "plan_total": "plan",
        "oper_total": "oper",
        "fact_total": "fact"
    }
    unit = "тыс"
    for i in ["rapira", "pnevmat", "total"]:
        for name in name_dict:
            format_data = await format_number(res_dict[i][name], unit)
            res_dict[i][f"{name_dict[name]}"] = format_data.number
            res_dict[i][f"{name_dict[name]}_unit"] = format_data.unit
        res_dict[i]["unit"] = unit

    return res_dict

async def tkachestvo_data(date, lnk_path=[lnk_paths.TKACHESTVO, lnk_paths.WEAVING]):
    res_dict = {
        "tkachestvo": {
            "day": {
                "f_ed": 0,
                "percent_work": 0
            },
            "until_day": {
                "f_ed": 0,
                "percent_work": 0
            },
            "fullness": None,
            "fullness_date": None,
        },
        "weaving": {
            "day": {
                "plan_total": 0,
                "oper_total": 0,
                "percent_plan": 0
                },
            "until_day": {
                "plan_total": 0,
                "oper_total": 0,
                "percent_plan": 0,
            },
            "fullness": None,
            "fullness_date": None,
            },
        "fullness": None,
        "fullness_date": None,
        "fullness_descriptions": None,
    }
    
    file_options = {
        "tkachestvo": {"index": 0, "sheets": {"sheet1": "оборуд"}, "name": "вып отгр (в сокращении)"},
        "weaving": {"index": 1, "sheets": {"sheet1": "База"}, "name": "вып отгр"},
    }
    try:
        # fullness and fullness_date ===============================================================================
        for active_file, options in file_options.items():
            if lnk_path == [lnk_paths.TKACHESTVO, lnk_paths.WEAVING]:
                src = pylnk3.parse(lnk_path[options["index"]]).path[3:].split("\\")
                src = os.path.join("/mnt", "harddrive", *src)
                dst = os.path.join(os.getcwd(), "data")
                await copy2(src, dst)
                workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
            else:
                workbook = CalamineWorkbook.from_filelike(lnk_path[options["index"]])
            
            for sheet in options["sheets"]:
                data = workbook.get_sheet_by_name(options["sheets"][sheet]).to_python()
                data = await clear_nan(data)
                # Tkachestvo fullness
                if options["sheets"][sheet] == "оборуд" and active_file == "tkachestvo":
                    for i in range(len(data)):
                        if type(data[i][0]) == datetime.date:
                            if data[i][0] == date:
                                if data[i][21] not in [0, "", " "]:
                                    print("test1", data[i][21])
                                    res_dict[active_file]["fullness"] = True
                                    res_dict[active_file]["fullness_date"] = date
                                else:
                                    res_dict[active_file]["fullness"] = False
                                break
                            
                    if res_dict[active_file]["fullness"] in [False, None]:
                        res_dict[active_file]["fullness_date"] = await get_last_fullness_date_tkachestvo()
                        
                        
                elif options["sheets"][sheet] == "База" and active_file == "weaving":
                    columns = {j:i for i, j in enumerate(data[1])}
                    fact_date = 0
                    for i in range(2, len(data)):
                        if type(data[i][columns["дата"]]) == datetime.date:
                            if data[i][columns["дата"]].month == date.month and data[i][columns["дата"]].year == date.year:
                                if data[i][columns["дата"]] == date:
                                    if type(data[i][columns["ф_пм"]]) in [float, int]:
                                        fact_date += data[i][columns["ф_пм"]]
                            if data[i][columns["дата"]] == (date + datetime.timedelta(days=1)):
                                print("test2", data[i][columns["дата"]], fact_date)
                                if fact_date != 0:
                                    res_dict[active_file]["fullness"] = True
                                    res_dict[active_file]["fullness_date"] = date
                                else:
                                    res_dict[active_file]["fullness"] = False
                                break
                    if res_dict[active_file]["fullness"] in [False, None]:
                        res_dict[active_file]["fullness_date"] = await get_last_fullness_date_weaving()
                        
        res_dict["fullness_date"] = min([res_dict[active_file]["fullness_date"] for active_file in file_options])
        res_dict["fullness"] = all([res_dict[active_file]["fullness"] for active_file in file_options])
        
        
        # Get data ===============================================================================
        for active_file, options in file_options.items():
            if lnk_path == [lnk_paths.TKACHESTVO, lnk_paths.WEAVING]:
                src = pylnk3.parse(lnk_path[options["index"]]).path[3:].split("\\")
                src = os.path.join("/mnt", "harddrive", *src)
                dst = os.path.join(os.getcwd(), "data")
                await copy2(src, dst)
                workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
            else:
                lnk_path[options["index"]].seek(0)
                workbook = CalamineWorkbook.from_filelike(lnk_path[options["index"]])
        
            for sheet in options["sheets"]:
                data = workbook.get_sheet_by_name(options["sheets"][sheet]).to_python()
                data = await clear_nan(data)
                last_date = res_dict["fullness_date"]
                flag = False
                if options["sheets"][sheet] == "оборуд" and active_file == "tkachestvo":
                    k = 0
                    for i in range(len(data)):
                        if type(data[i][0]) == datetime.date:
                            if data[i][0].year == date.year and data[i][0].month == date.month:
                                if type(data[i][21]) in [float, int]:
                                    res_dict[active_file]["until_day"]["f_ed"] += round(data[i][21])
                                    k+=1
                                if type(data[i][27]) in [float, int]:
                                    res_dict[active_file]["until_day"]["percent_work"] += round(data[i][27]*100, 1)
                                
                                if data[i][0] == date:
                                    if type(data[i][21]) in [float, int]:
                                        res_dict[active_file]["day"]["f_ed"] = round(data[i][21])
                                    if type(data[i][27]) in [float, int]:
                                        res_dict[active_file]["day"]["percent_work"] = round(data[i][27]*100, 1)
                                        
                                    res_dict[active_file]["until_day"]["f_ed"] = await calc_average(res_dict[active_file]["until_day"]["f_ed"], k, 2)
                                    res_dict[active_file]["until_day"]["percent_work"] = await calc_average(res_dict[active_file]["until_day"]["percent_work"], k, 2)
                                    flag = True
                                    break
                    if not flag:
                        res_dict[active_file]["until_day"]["f_ed"] = await calc_average(res_dict[active_file]["until_day"]["f_ed"], k, 2)
                        res_dict[active_file]["until_day"]["percent_work"] = await calc_average(res_dict[active_file]["until_day"]["percent_work"], k, 2)
                        
                elif options["sheets"][sheet] == "База" and active_file == "weaving":
                    columns = {j:i for i, j in enumerate(data[1])}
                    fact_date = 0
                    for i in range(2, len(data)):
                        if type(data[i][columns["дата"]]) == datetime.date:
                            if data[i][columns["дата"]].month == date.month and data[i][columns["дата"]].year == date.year:
                                if data[i][columns["План/ОП"]] == "План" and data[i][columns["дата"]] <= date:
                                    if type(data[i][columns["п_м_м_уточ"]]) in [float, int]:
                                        res_dict[active_file]["until_day"]["plan_total"] += data[i][columns["п_м_м_уточ"]]
                                        if data[i][columns["дата"]] == date:
                                            res_dict[active_file]["day"]["plan_total"] += data[i][columns["п_м_м_уточ"]]
                                            
                                if data[i][columns["План/ОП"]] == "ОП" and data[i][columns["дата"]] <= date:
                                    if type(data[i][columns["ОП_п_м_м_уточ"]]) in [float, int]:
                                        res_dict[active_file]["until_day"]["oper_total"] += data[i][columns["ОП_п_м_м_уточ"]]
                                        if data[i][columns["дата"]] == date:
                                            res_dict[active_file]["day"]["oper_total"] += data[i][columns["ОП_п_м_м_уточ"]]
                                            
                    res_dict[active_file]["day"]["percent_plan"] = await calc_percent_plan(res_dict[active_file]["day"]["plan_total"], res_dict[active_file]["day"]["oper_total"])
                    res_dict[active_file]["until_day"]["percent_plan"] = await calc_percent_plan(res_dict[active_file]["until_day"]["plan_total"], res_dict[active_file]["until_day"]["oper_total"])        
        res_dict = {
            "tkachestvo": {
                "day": {
                    "f_ed": res_dict["tkachestvo"]["day"]["f_ed"],
                    "percent_work": res_dict["tkachestvo"]["day"]["percent_work"],
                },
                "until_day": {
                    "f_ed": res_dict["tkachestvo"]["until_day"]["f_ed"],
                    "percent_work": res_dict["tkachestvo"]["until_day"]["percent_work"],
                },
                "fullness": res_dict["tkachestvo"]["fullness"],
                "fullness_date": res_dict["tkachestvo"]["fullness_date"],
            },
            "weaving": {
                "day": {
                    "percent_plan": res_dict["weaving"]["day"]["percent_plan"],
                    },
                "until_day": {
                    "percent_plan": res_dict["weaving"]["until_day"]["percent_plan"]
                    },
                "fullness": res_dict["weaving"]["fullness"],
                "fullness_date": res_dict["weaving"]["fullness_date"],
            },
            "fullness": res_dict["fullness"],
            "fullness_date": res_dict["fullness_date"],
            "fullness_descriptions": res_dict["fullness_descriptions"],
        }     
        return await descriptions_creator(res_dict, file_name="Ткачество", date=res_dict["fullness_date"])
    
    except FileNotFoundError:
        return await descriptions_creator(res_dict, error=Exception, file_name=options["name"])
    except Exception:
        print(traceback.format_exc())
        return await descriptions_creator(res_dict, error=Exception, file_name="")

async def yarn_data(date, lnk_path=lnk_paths.YARN):
    res_dict = {
        "field_68": {
            "remainder_total": 0,  # Остаток
            "consumption_total": 0,  # Расход
            "KTZ": 0,  # КТЗ
            "KTZ_status": 0,
            "payment": None,  # План оплата
            "admission": None,  # Поступление
        },
        "field_20": {
            "remainder_total": 0,
            "consumption_total": 0,
            "KTZ": 0,
            "KTZ_status": 0,
            "payment": None,
            "admission": None,
        },
        "field_54_2": {
            "remainder_total": 0,
            "consumption_total": 0,
            "KTZ": 0,
            "KTZ_status": 0,
            "payment": None,
            "admission": None,
        },
        "field_40_2": {
            "remainder_total": 0,
            "consumption_total": 0,
            "KTZ": 0,
            "KTZ_status": 0,
            "payment": None,
            "admission": None,
        },
        "field_25_smes": {
            "remainder_total": 0,
            "consumption_total": 0,
            "KTZ": 0,
            "KTZ_status": 0,
            "payment": None,
            "admission": None,
        },
        "total": {
            "remainder_total": 0,
            "consumption_total": 0,
            "arrival_total": 0,
            "KTZ": 0,
            "KTZ_status": 0
        },
        "fullness": None,
        "fullness_date": None,
        "fullness_descriptions": None,
    }
    
    sheets = {
        "68": "field_68",
        "20": "field_20",
        "54-2": "field_54_2",
        "40-2": "field_40_2",
        "25 смес": "field_25_smes",
        "Сводная": "total"
    }
    try:
        if lnk_path == lnk_paths.YARN:
            src = pylnk3.parse(lnk_path).path[3:].split("\\")
            src = os.path.join("/mnt", "harddrive", *src)
            dst = os.path.join(os.getcwd(), "data")
            await copy2(src, dst)
            workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
        else:
            workbook = CalamineWorkbook.from_filelike(lnk_path)

        for sheet_name in sheets:
            data = workbook.get_sheet_by_name(sheet_name).to_python()
            data = await clear_nan(data)
            for i in range(len(data)):
                if data[i][0] == date:
                    if sheets[sheet_name] != "total":
                        if type(data[i][5]) in [int, float]:
                            res_dict[sheets[sheet_name]]["remainder_total"] = data[i][5]
                            
                        if type(data[i][2]) in [int, float]:
                            res_dict[sheets[sheet_name]]["consumption_total"] = data[i][2]
                            
                        if type(data[i][12]) in [float, int]:
                            res_dict[sheets[sheet_name]]["KTZ"] = data[i][12]
                            if (round(data[i][12])) > 15:
                                res_dict[sheets[sheet_name]]["KTZ_status"] = 2
                            elif (round(data[i][12])) <= 15 and (round(data[i][12])) > 12:
                                res_dict[sheets[sheet_name]]["KTZ_status"] = 1
                            else:
                                res_dict[sheets[sheet_name]]["KTZ_status"] = 0
                                
                        for j in range(i, len(data)):
                            if (data[j][9] not in [0, " ", ""]) and data[j][10] == "":
                                res_dict[sheets[sheet_name]]["payment"] = data[j][9]
                                break
                        for j in range(i, len(data)):
                            if data[j][1] not in [0, ""]:
                                res_dict[sheets[sheet_name]]["admission"] = data[j][0]
                                break
                    else:
                        if type(data[i][20]) in [int, float]:
                            res_dict[sheets[sheet_name]]["remainder_total"] = data[i][20]
                            
                        if type(data[i][17]) in [int, float]:
                            res_dict[sheets[sheet_name]]["consumption_total"] = data[i][17]
                            
                        if type(data[i][21]) in [float, int]:
                            res_dict[sheets[sheet_name]]["KTZ"] = data[i][21]
                            if (round(data[i][12])) > 15:
                                res_dict[sheets[sheet_name]]["KTZ_status"] = 2
                            elif (round(data[i][12])) <= 15 and (round(data[i][12])) > 12:
                                res_dict[sheets[sheet_name]]["KTZ_status"] = 1
                            else:
                                res_dict[sheets[sheet_name]]["KTZ_status"] = 0
                                
        backup_data = await get_data_backup(date)
        if backup_data:
            for i in sheets.values():
                if i == "total":
                    continue
                if backup_data[i]["consumption_total"] != res_dict[i]["consumption_total"]:
                    res_dict["fullness"] = True
                    res_dict["fullness_date"] = date
                    break
                else:
                    res_dict["fullness"] = False
                    res_dict["fullness_date"] = await last_fullness_date_yarn()
                    print(await last_fullness_date_yarn())
        else:
            res_dict["fullness"] = None
            res_dict["fullness_date"] = await last_fullness_date_yarn()
        
        name_dict = {
            "remainder_total": "remainder",
            "consumption_total": "consumption",
        }
        unit = "тыс"
        for i in [*sheets.values(), "total"]:
            for name in name_dict:
                format_data = await format_number(res_dict[i][name], unit)
                res_dict[i][f"{name_dict[name]}"] = format_data.number
                res_dict[i][f"{name_dict[name]}_unit"] = "тонны"#format_data.unit
            res_dict[i]["unit"] = "тонны"
        return await descriptions_creator(res_dict, date=res_dict["fullness_date"], file_name="Пряжа")
    
    except Exception:
        print(traceback.format_exc())
        return await descriptions_creator(res_dict, error=FileNotFoundError, file_name="Пряжа")
    

async def yarn_data_backup(date):
    res_dict = {
        "field_68": {
            "consumption_total": 0,  # Расход
        },
        "field_20": {
            "consumption_total": 0,
        },
        "field_54_2": {
            "consumption_total": 0,
        },
        "field_40_2": {
            "consumption_total": 0,
        },
        "field_25_smes": {
            "consumption_total": 0,
        },
    }
    sheets = {
        "68": "field_68",
        "20": "field_20",
        "54-2": "field_54_2",
        "40-2": "field_40_2",
        "25 смес": "field_25_smes",
    }
    lnk_path = lnk_paths.YARN
    src = pylnk3.parse(lnk_path).path[3:].split("\\")
    src = os.path.join("/mnt", "harddrive", *src)
    dst = os.path.join(os.getcwd(), "data")
    await copy2(src, dst)
    workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
    for sheet_name in sheets:
        data = workbook.get_sheet_by_name(sheet_name).to_python()
        data = await clear_nan(data)
        for i in range(len(data)):
            if data[i][0] == date:
                if type(data[i][2]) in [int, float]:
                    res_dict[sheets[sheet_name]]["consumption_total"] = data[i][2]
    return res_dict
        
        
async def dyeing_data(date, lnk_path=lnk_paths.DYEING):
    res_dict = {
        "fullness": None,
        "saver": {
            "plan_total": 0,
            "oper_total": 0,
            "fact_total": 0,
        },
        "dmc1": {
            "plan_total": 0,
            "oper_total": 0,
            "fact_total": 0,
        },
        "dmc2": {
            "plan_total": 0,
            "oper_total": 0,
            "fact_total": 0,
        },
        "kusters1": {
            "plan_total": 0,
            "oper_total": 0,
            "fact_total": 0,
        },
        "kusters2": {
            "plan_total": 0,
            "oper_total": 0,
            "fact_total": 0,
        },
        "henriksen1": {
            "plan_total": 0,
            "oper_total": 0,
            "fact_total": 0,
        },
        "henriksen2": {
            "plan_total": 0,
            "oper_total": 0,
            "fact_total": 0,
        },
        "total": {
            "plan_total": 0,
            "oper_total": 0,
            "fact_total": 0,
        },
    }
    equipment = {
        "Saver": "saver",
        "ДМС 1": "dmc1",
        "ДМС 2": "dmc2",
        "Кюстерс 1": "kusters1",
        "Кюстерс 2": "kusters2",
        "Хенриксен 1": "henriksen1",
        "Хенриксен 2": "henriksen2",
    }

    if lnk_path == lnk_paths.DYEING:
        src = pylnk3.parse(lnk_path).path[3:].split("\\")
        src = os.path.join("/mnt", "harddrive", *src)
        dst = os.path.join(os.getcwd(), "data")
        await copy2(src, dst)
        workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
    else:
        workbook = CalamineWorkbook.from_filelike(lnk_path)

    data = workbook.get_sheet_by_name("База").to_python()
    # data = await clear_nan(data)
    columns = {j:i for i, j in enumerate(data[1])}
    
    for i in range(len(data)):
        if data[i][0] == date:
            # print(i+1, {col: data[i][columns[col]] for col in columns})
            if data[i][5] in equipment:
                if type(data[i][16]) in [float, int]:
                    res_dict[equipment[data[i][5]]]["plan_total"] += data[i][16]
                if type(data[i][17]) in [float, int]:
                    res_dict[equipment[data[i][5]]]["oper_total"] += data[i][17]
                if type(data[i][18]) in [float, int]:
                    res_dict[equipment[data[i][5]]]["fact_total"] += data[i][18]

                if type(data[i][16]) in [float, int]:
                    res_dict["total"]["plan_total"] += data[i][16]
                if type(data[i][17]) in [float, int]:
                    res_dict["total"]["oper_total"] += data[i][17]
                if type(data[i][18]) in [float, int]:
                    res_dict["total"]["fact_total"] += data[i][18]
    
    if res_dict["total"]["fact_total"] != 0:
        res_dict["fullness"] = True
    else: res_dict["fullness"] = False
    name_dict = {
        "plan_total": "plan",
        "oper_total": "oper",
        "fact_total": "fact"
    }
    unit = "тыс"
    for i in [*equipment.values(), "total"]:
        for name in name_dict:
            format_data = await format_number(res_dict[i][name], unit)
            res_dict[i][f"{name_dict[name]}"] = format_data.number
            res_dict[i][f"{name_dict[name]}_unit"] = format_data.unit
        res_dict[i]["unit"] = unit
    return res_dict


async def sales_plan_data(lnk_path=lnk_paths.SALES_PLAN):
    res_dict = {
        "shipments": {}, # Отгрузки
        "receipts": {}, # Поступления
        "fullness": None
        }   

    sectors = {
        "ИТОГО": "total",
        "Ткани": "fabrics",
        "Изделия": "goods",
        "Е ком": "e_com",
        "Струничева Д.": "manager1",
        "Ванюшина С.": "manager2",
        "Перьева Ю.": "manager3",
    }

    managers = [
        "Зайцева Т.А.",
        "Менькова Т.М.",
        "Васильева О.",
        "Магазин",
        "Попова Е.Д.",
    ]

    if lnk_path == lnk_paths.SALES_PLAN:
        src = pylnk3.parse(lnk_path).path[3:].split("\\")
        src = os.path.join("/mnt", "harddrive", *src)
        dst = os.path.join(os.getcwd(), "data")
        await copy2(src, dst)
        workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
    else:
        workbook = CalamineWorkbook.from_filelike(lnk_path)

    data = workbook.get_sheet_by_name("свод").to_python()
    data = await clear_nan(data)
    columns = {j:i for i, j in enumerate(data[2])}
    if (data[3][columns["Дата заполнения"]] >= (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).date()):
        res_dict["fullness"] = True
    else:
        res_dict["fullness"] = False
    active_manager = ""
    active_sector = ""
    for i in range(3, len(data)):

        if data[i][0] in sectors:
            active_sector = sectors[data[i][0]]
            res_dict["shipments"][active_sector] = {"managers": [], "total": {}}
            res_dict["receipts"][active_sector] = {"managers": [], "total": {}}

            res_dict["shipments"][active_sector]["total"]["plan"] = data[i][1]
            res_dict["shipments"][active_sector]["total"]["oper"] = data[i][9]
            res_dict["shipments"][active_sector]["total"]["fact"] = data[i][6]

            res_dict["receipts"][active_sector]["total"]["plan"] = data[i][12]
            res_dict["receipts"][active_sector]["total"]["oper"] = data[i][19]
            res_dict["receipts"][active_sector]["total"]["fact"] = data[i][17]
            active_manager = ""
        if data[i][0] in managers:
            active_manager = data[i][0]
            res_dict["shipments"][active_sector]["managers"].append(
                {"manager": active_manager}
            )
            res_dict["receipts"][active_sector]["managers"].append(
                {"manager": active_manager}
            )
            for manager_id in range(
                len(res_dict["receipts"][active_sector]["managers"])
            ):
                if (
                    res_dict["receipts"][active_sector]["managers"][manager_id][
                        "manager"
                    ]
                    == active_manager
                ):
                    res_dict["shipments"][active_sector]["managers"][manager_id][
                        "plan"
                    ] = data[i][1]
                    res_dict["shipments"][active_sector]["managers"][manager_id][
                        "oper"
                    ] = data[i][9]
                    res_dict["shipments"][active_sector]["managers"][manager_id][
                        "fact"
                    ] = data[i][6]

                    res_dict["receipts"][active_sector]["managers"][manager_id][
                        "plan"
                    ] = data[i][12]
                    res_dict["receipts"][active_sector]["managers"][manager_id][
                        "oper"
                    ] = data[i][19]
                    res_dict["receipts"][active_sector]["managers"][manager_id][
                        "fact"
                    ] = data[i][17]

    return res_dict

async def sales_data(date, lnk_path=[lnk_paths.SALES_PLAN, lnk_paths.CF, lnk_paths.MARGIN]):
    res_dict = {
        "sales": {
            # "fullness": None,
            # "fullness_date": None,
            "plan": 0,
            "oper": 0,
            "deviation": 0,
            },
        "cash_flow": {
            "fullness": None,
            "fullness_date": None,
            "plan": 0,
            "oper": 0,
            "fact": 0,
            "deviation": 0,
        },
        "margin": {
            "fullness": None,
            "fullness_date": None,
            "fact": 0
        },
        "fullness": None,
        "fullness_date": None
    }
    
    file_options = {
        "sales": {"index": 0, "sheets": {"sheet1": "свод"}},
        "cash_flow": {"index": 1, "sheets": {"sheet1": "оперплан", "sheet2": "план"}},
        "margin": {"index": 2, "sheets": {"sheet1": "база"}}
    }
    try:
        yesterday = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).date()
        
        #fullness and fullness_date
        for active_file, options in file_options.items():
            if lnk_path == [lnk_paths.SALES_PLAN, lnk_paths.CF, lnk_paths.MARGIN]:
                src = pylnk3.parse(lnk_path[options["index"]]).path[3:].split("\\")
                src = os.path.join("/mnt", "harddrive", *src)
                dst = os.path.join(os.getcwd(), "data")
                await copy2(src, dst)
                workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
            else:
                workbook = CalamineWorkbook.from_filelike(lnk_path[options["index"]])
            
            for sheet in options["sheets"]:
                data = workbook.get_sheet_by_name(options["sheets"][sheet]).to_python()
                data = await clear_nan(data)
                
                # if options["sheets"][sheet] == "свод" and active_file == "sales":
                #     columns = {j:i for i, j in enumerate(data[2])}
                #     if (data[3][columns["Дата заполнения"]] >= yesterday):
                #         res_dict[active_file]["fullness"] = True
                #     else:
                #         res_dict[active_file]["fullness"] = False
                #     res_dict[active_file]["fullness_date"] = data[3][columns["Дата заполнения"]]
            
                if options["sheets"][sheet] == "база" and active_file == "margin":
                    dates_set = set()
                    for i in range(len(data)):
                        if type(data[i][3]) == datetime.date:
                            dates_set.add(data[i][3])
                            
                    res_dict[active_file]["fullness_date"] = max(dates_set)
                    if res_dict[active_file]["fullness_date"] >= date:
                        res_dict[active_file]["fullness"] = True
                    else:
                        res_dict[active_file]["fullness"] = False
                        res_dict[active_file]["fullness_date"] = await last_fullness_date_sales()
                        
                if options["sheets"][sheet] == "оперплан" and active_file == "cash_flow":
                    columns = {j:i for i, j in enumerate(data[1])}
                    if date.day in columns:
                        res_dict[active_file]["fullness"] = True
                        res_dict[active_file]["fullness_date"] = date
                    else:
                        res_dict[active_file]["fullness"] = False
                        res_dict[active_file]["fullness_date"] = await last_fullness_date_sales()
                    # for col in columns:
                    #     if type(col) == int:
                    #         try:
                    #             res_dict[active_file]["fullness_date"] = yesterday.replace(day = int(col))
                    #         except Exception: print(traceback.format_exc())
                    #         try:
                    #             if d in str(col):
                    #                 if "*" not in str(col):
                    #                     res_dict[active_file]["fullness"] = True
                    #                 else:
                    #                     res_dict[active_file]["fullness"] = False
                    #                     res_dict[active_file]["fullness_date"] = await last_fullness_date_sales()
                    #         except Exception: print(traceback.format_exc())
        res_dict["fullness_date"] = min(
            res_dict[active_file]["fullness_date"] 
            for active_file in file_options 
            if "fullness_date" in res_dict[active_file]
        )
        res_dict["fullness"] = all(res_dict[active_file]["fullness"] 
            for active_file in file_options 
            if "fullness" in res_dict[active_file]
        )
        # Get data from files
        for active_file, options in file_options.items():
            if lnk_path == [lnk_paths.SALES_PLAN, lnk_paths.CF, lnk_paths.MARGIN]:
                src = pylnk3.parse(lnk_path[options["index"]]).path[3:].split("\\")
                src = os.path.join("/mnt", "harddrive", *src)
                dst = os.path.join(os.getcwd(), "data")
                await copy2(src, dst)
                workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
            else:
                lnk_path[options["index"]].seek(0)
                workbook = CalamineWorkbook.from_filelike(lnk_path[options["index"]])
                
            for sheet in options["sheets"]:
                data = workbook.get_sheet_by_name(options["sheets"][sheet]).to_python()
                data = await clear_nan(data)
                if options["sheets"][sheet] == "свод" and active_file == "sales":
                    res_dict[active_file]["plan"] = data[3][1]
                    res_dict[active_file]["oper"] = data[3][9]
                    res_dict["cash_flow"]["plan"] = data[3][12]
                    res_dict["cash_flow"]["oper"] = data[3][19]
                    
                if options["sheets"][sheet] == "база" and active_file == "margin":
                    date = res_dict["fullness_date"]
                    for i in range(len(data)):
                        if type(data[i][18]) in [float, int]:
                            if type(data[i][3]) == datetime.date:
                                if data[i][3].month == date.month and data[i][3].year == date.year:
                                    if data[i][3] <= (date+datetime.timedelta(days=1)):
                                        res_dict[active_file]["fact"] += data[i][18]

                    
                if options["sheets"][sheet] == "оперплан" and active_file == "cash_flow":
                    columns = {j:i for i, j in enumerate(data[1])}
                    try:
                        for col in columns:
                            if type(col) in [float, int]:
                                if col <= date.day:
                                    res_dict[active_file]["fact"] += data[4][columns[col]]
                    except Exception: print(traceback.format_exc())
                        
            if "deviation" in res_dict[active_file]:
                res_dict[active_file]["deviation"] = (await calc_deviation(res_dict[active_file]["plan"], res_dict[active_file]["oper"]))
        res_dict = {
            "sales": {
                "plan": res_dict["sales"]["plan"],
                "oper": res_dict["sales"]["oper"],
                "fact": res_dict["margin"]["fact"],
                "deviation": res_dict["sales"]["deviation"],
                },
            "cash_flow": {
                "fullness": res_dict["cash_flow"]["fullness"],
                "fullness_date": res_dict["cash_flow"]["fullness_date"],
                "plan": res_dict["cash_flow"]["plan"],
                "oper": res_dict["cash_flow"]["oper"],
                "fact": res_dict["cash_flow"]["fact"],
                "deviation": res_dict["cash_flow"]["deviation"],
            },
            "fullness": res_dict["fullness"],
            "fullness_date": res_dict["fullness_date"],
            "fullness_descriptions": None,
            
        }
        return await descriptions_creator(res_dict, date=res_dict["fullness_date"], file_name="Продажи")
    except Exception:
        print(traceback.format_exc())
        res_dict = {
        "sales": {
            "plan": res_dict["sales"]["plan"],
            "oper": res_dict["sales"]["oper"],
            "fact": res_dict["margin"]["fact"],
            "deviation": res_dict["sales"]["deviation"],
            },
        "cash_flow": {
            "fullness": res_dict["cash_flow"]["fullness"],
            "fullness_date": res_dict["cash_flow"]["fullness_date"],
            "plan": res_dict["cash_flow"]["plan"],
            "oper": res_dict["cash_flow"]["oper"],
            "fact": res_dict["cash_flow"]["fact"],
            "deviation": res_dict["cash_flow"]["deviation"],
        },
        "fullness": res_dict["fullness"],
        "fullness_date": last_fullness_date_sales,
        "fullness_descriptions": None,
        }
        return await descriptions_creator(res_dict, error=Exception, date=res_dict["fullness_date"], file_name="Продажи")
                
                

async def finishing_data(lnk_path=lnk_paths.FINISHING):
    res_dict = {
        "fullness": None,
        "dyed": {  # Крашенная
            "plan": 0,
            "operSorting": 0,
            "operFinishing": 0,
            "factSorting": 0,
            "factFinishing": 0,
            "proc_planFinishing": 0,
        },
        "bleached": {  # Отбеленная
            "plan": 0,
            "operSorting": 0,
            "operFinishing": 0,
            "factSorting": 0,
            "factFinishing": 0,
            "proc_planFinishing": 0,
        },
        "multicolored": {  # Пестроткань
            "plan": 0,
            "operSorting": 0,
            "operFinishing": 0,
            "factSorting": 0,
            "factFinishing": 0,
            "proc_planFinishing": 0,
        },
        "stamp": {  # Печать
            "plan": 0,
            "operSorting": 0,
            "operFinishing": 0,
            "factSorting": 0,
            "factFinishing": 0,
            "proc_planFinishing": 0,
        },
        "washed": {  # промытая
            "plan": 0,
            "operSorting": 0,
            "operFinishing": 0,
            "factSorting": 0,
            "factFinishing": 0,
            "proc_planFinishing": 0,
        },
        "raw": {  # суровая
            "plan": 0,
            "operSorting": 0,
            "operFinishing": 0,
            "factSorting": 0,
            "factFinishing": 0,
            "proc_planFinishing": 0,
        },
        "total": {
            "plan": 0,
            "operSorting": 0,
            "operFinishing": 0,
            "factSorting": 0,
            "factFinishing": 0,
            "proc_planFinishing": 0,
        },
    }

    fabric_type_dict = {
        "крашеная": "dyed",
        "отбеленная": "bleached",
        "пестроткань": "multicolored",
        "печать": "stamp",
        "промытая": "washed",
        "суровая": "raw",
    }

    if lnk_path == lnk_paths.FINISHING:
        src = pylnk3.parse(lnk_path).path[3:].split("\\")
        src = os.path.join("/mnt", "harddrive", *src)
        dst = os.path.join(os.getcwd(), "data")
        await copy2(src, dst)
        workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
    else:
        workbook = CalamineWorkbook.from_filelike(lnk_path)

    data = workbook.get_sheet_by_name("Отделка").to_python()
    data = await clear_nan(data)
    columns = {
        j: i for i, j in enumerate([x for x in data[2] if x not in ["", " ", None]])
    }
    flag = data[1]
    date_dict = {}
    for i in columns:
        d = str((datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).day)
        if len(d) == 2:
            pass
        else:
            d = "0" + d
        if d in i and len(i) == 3:
            date_dict[i] = columns[i]
    if all([True if flag[date_dict[i]] == "факт" else False for i in date_dict]):
        res_dict["fullness"] = True
    else: res_dict["fullness"] = False
        

    for i in range(4, len(data)):
        try:
            fabric_type = fabric_type_dict[data[i][6]]
            # plan =============================================================
            if type(data[i][14]) in [float, int]:
                res_dict[fabric_type]["plan"] += data[i][14]
                res_dict["total"]["plan"] += data[i][14]
            # operSorting =============================================================
            if type(data[i][32]) in [float, int]:
                res_dict[fabric_type]["operSorting"] += data[i][32]
                res_dict["total"]["operSorting"] += data[i][32]
            # factSorting =============================================================
            if type(data[i][31]) in [float, int]:
                res_dict[fabric_type]["factSorting"] += data[i][31]
                res_dict["total"]["factSorting"] += data[i][31]
            # operFinishing =============================================================
            if type(data[i][70]) in [float, int]:
                res_dict[fabric_type]["operFinishing"] += data[i][70]
                res_dict["total"]["operFinishing"] += data[i][70]
            # factFinishing =============================================================
            if type(data[i][65]) in [float, int]:
                res_dict[fabric_type]["factFinishing"] += data[i][65]
                res_dict["total"]["factFinishing"] += data[i][65]
        except KeyError:
            pass

    for fabric_type in [*fabric_type_dict.values(), "total"]:
        try:
            res_dict[fabric_type]["proc_planFinishing"] = (
                res_dict[fabric_type]["factSorting"] / (res_dict[fabric_type]["plan"])
            ) * 100
        except ZeroDivisionError:
            pass

    return res_dict


async def sewing_data(lnk_path=lnk_paths.SEWING):
    res_dict = {
        "fullness": None,
        "kpb": {
            "inside": {
                "clothes": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "kitchen": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "bedroom": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "shirts": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "total": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
            },
            "outside": {
                "clothes": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "kitchen": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "bedroom": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "shirts": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "total": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
            },
            "total": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
        },
        "rub": {
            "inside": {
                "clothes": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "kitchen": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "bedroom": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "shirts": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "total": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
            },
            "outside": {
                "clothes": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "kitchen": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "bedroom": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "shirts": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
                "total": {"plan_total": 0, "oper_total": 0, "fact_total": 0},
            },
            "total": {"plan_total": 0, "oper_total": 0, "fact_total": 0}
        },
    }
    
    production_dict = {
        "швейка": "inside",
        "сторона": "outside"
    }
    product_dict = {
        "Одежда": "clothes",
        "Кухня": "kitchen",
        "Спальня": "bedroom",
        "Рубашки": "shirts",
    }

    if lnk_path == lnk_paths.SEWING:
        src = pylnk3.parse(lnk_path).path[3:].split("\\")
        src = os.path.join("/mnt", "harddrive", *src)
        dst = os.path.join(os.getcwd(), "data")
        await copy2(src, dst)
        workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
    else:
        workbook = CalamineWorkbook.from_filelike(lnk_path)
        
    data = workbook.get_sheet_by_name("дни").to_python()
    data = await clear_nan(data)
    days = data[1]
    if (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).day in days:
        res_dict["fullness"] = True
    else:
        res_dict["fullness"] = False
    
    
    data = workbook.get_sheet_by_name("план").to_python()
    data = await clear_nan(data)   
    columns = {j:i for i, j in enumerate(data[5])}
    
    for i in range(6, len(data)):
        if data[i][columns["пр-во*"]] in production_dict:
            production = production_dict[data[i][columns["пр-во*"]]]
            if data[i][columns["группа"]] in product_dict:
                product = product_dict[data[i][columns["группа"]]]
                # КПБ ==================
                if type(data[i][columns["План КПБ"]]) in [float, int]:
                    res_dict["kpb"][production][product]["plan_total"] += data[i][columns["План КПБ"]]
                    res_dict["kpb"][production]["total"]["plan_total"] += data[i][columns["План КПБ"]]
                    res_dict["kpb"]["total"]["plan_total"] += data[i][columns["План КПБ"]]
                if type(data[i][columns["ОП ВСЕГО, кпб"]]) in [float, int]: 
                    res_dict["kpb"][production][product]["oper_total"] += data[i][columns["ОП ВСЕГО, кпб"]]
                    res_dict["kpb"][production]["total"]["oper_total"] += data[i][columns["ОП ВСЕГО, кпб"]]
                    res_dict["kpb"]["total"]["oper_total"] += data[i][columns["ОП ВСЕГО, кпб"]]
                if type(data[i][columns["факт КПБ"]]) in [float, int]:
                    res_dict["kpb"][production][product]["fact_total"] += data[i][columns["факт КПБ"]]
                    res_dict["kpb"][production]["total"]["fact_total"] += data[i][columns["факт КПБ"]]
                    res_dict["kpb"]["total"]["fact_total"] += data[i][columns["факт КПБ"]]
                    
                # Руб ==================
                if type(data[i][columns["План руб"]]) in [float, int]:
                    res_dict["rub"][production][product]["plan_total"] += data[i][columns["План руб"]]
                    res_dict["rub"][production]["total"]["plan_total"] += data[i][columns["План руб"]]
                    res_dict["rub"]["total"]["plan_total"] += data[i][columns["План руб"]]
                if type(data[i][columns["ОП ВСЕГО, руб"]]) in [float, int]:
                    res_dict["rub"][production][product]["oper_total"] += data[i][columns["ОП ВСЕГО, руб"]]
                    res_dict["rub"][production]["total"]["oper_total"] += data[i][columns["ОП ВСЕГО, руб"]]
                    res_dict["rub"]["total"]["oper_total"] += data[i][columns["ОП ВСЕГО, руб"]]
                if type(data[i][columns["факт, руб"]]) in [float, int]:
                    res_dict["rub"][production][product]["fact_total"] += data[i][columns["факт, руб"]]
                    res_dict["rub"][production]["total"]["fact_total"] += data[i][columns["факт, руб"]]
                    res_dict["rub"]["total"]["fact_total"] += data[i][columns["факт, руб"]]
                    
    name_dict = {
        "plan_total": "plan",
        "oper_total": "oper",
        "fact_total": "fact",
    }
    units = {
        "kpb": "тыс",
        "rub": "млн"}
    for i in ["kpb", "rub"]:
        for production in production_dict.values():
            for product in [*product_dict.values(), "total"]:
                for name in name_dict:
                    format_data = await format_number(res_dict[i][production][product][name], units[i])
                    res_dict[i][production][product][f"{name_dict[name]}"] = format_data.number
                    res_dict[i][production][product][f"{name_dict[name]}_unit"] = format_data.unit
                res_dict[i][production][product]["unit"] = units[i]
        for name in name_dict:
            format_data = await format_number(res_dict[i]["total"][name], units[i])
            res_dict[i]["total"][f"{name_dict[name]}"] = format_data.number
            res_dict[i]["total"][f"{name_dict[name]}_unit"] = format_data.unit
            res_dict[i]["total"]["unit"] = units[i]
    return res_dict
            
            
async def warp_data(date: datetime.datetime, lnk_path=lnk_paths.WARP):
    res_dict = {
        "cotton": {
            },
        "aramid": {
            },
        "linen": {
            },
        "fullness": None
    }
    
    group_dict = {
        "х/б": "cotton",
        "арамид": "aramid",
        "лен": "linen",
    }

    if lnk_path == lnk_paths.WARP:
        src = pylnk3.parse(lnk_path).path[3:].split("\\")
        src = os.path.join("/mnt", "harddrive", *src)
        dst = os.path.join(os.getcwd(), "data")
        await copy2(src, dst)
        workbook = CalamineWorkbook.from_path(dst + "/" + src.split("/")[-1])
    else:
        workbook = CalamineWorkbook.from_filelike(lnk_path)

    data = workbook.get_sheet_by_name("Сновка_База").to_python()
    data = await clear_nan(data)
    columns = {j: i for i, j in enumerate(data[1])}
    
    if datetime.date(int(data[-1][0]), config.month_dict_rev[(data[-1][1]).lower()], int(data[-1][2])) >= ((datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).date()):
        res_dict["fullness"] = True
    else:
        res_dict["fullness"] = True
        
    for i in range(2, len(data)):
        loc_date = datetime.date(int(data[i][0]), config.month_dict_rev[(data[i][1]).lower()], int(data[i][2]))
        if date == loc_date:
            if (data[i][10]).lower() in group_dict:
                try:
                    res_dict[group_dict[(data[i][10]).lower()]][data[i][11]]
                except: res_dict[group_dict[(data[i][10]).lower()]][data[i][11]] = {}
                try:
                    res_dict[group_dict[(data[i][10]).lower()]][data[i][11]]["count"] += 1
                except: res_dict[group_dict[(data[i][10]).lower()]][data[i][11]]["count"] = 1
                
                res_dict[group_dict[(data[i][10]).lower()]][data[i][11]]["manufacturer"] = data[i][9] # Производитель
                res_dict[group_dict[(data[i][10]).lower()]][data[i][11]]["lot"] = data[i][11] # Лот
                
                try:
                    res_dict[group_dict[(data[i][10]).lower()]][data[i][11]]["meters"] += data[i][14] # Кол-во метров
                except: res_dict[group_dict[(data[i][10]).lower()]][data[i][11]]["meters"] = data[i][14]
                try:
                    res_dict[group_dict[(data[i][10]).lower()]][data[i][11]]["break_count"] += data[i][17] #Кол-во обрывов
                except: res_dict[group_dict[(data[i][10]).lower()]][data[i][11]]["break_count"] = data[i][17]
                try:
                    res_dict[group_dict[(data[i][10]).lower()]][data[i][11]]["breakage_rate"] += data[i][18] #Обрывность
                except:
                    res_dict[group_dict[(data[i][10]).lower()]][data[i][11]]["breakage_rate"] = data[i][18]
    
    for i in group_dict.values():
        count = 0
        break_count = 0
        breakage_rate = 0
        for number_fabric in res_dict[i]:
            count += 1
            break_count += res_dict[i][number_fabric]["break_count"]
            breakage_rate += res_dict[i][number_fabric]["breakage_rate"]
        try:
            breakage_rate = round(breakage_rate/count, 2)
        except: breakage_rate = 0
        res_dict[i]["total"] = {
            "count": count,
            "break_count": break_count,
            "breakage_rate": breakage_rate,
        }
    return res_dict
        
            