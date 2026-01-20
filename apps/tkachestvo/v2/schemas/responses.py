from ..models import Tkachestvo_model
from pydantic import BaseModel


class TkachestvoGetStatsResponse(BaseModel):
	result: list[Tkachestvo_model, None]
	total: int | None = None

def res_generator(res_dict):
	res_dict = {
		"day":{
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
					"unit": "",
				},
				"percent_oper": {
					"value": 0,
					"value_total": 0,
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
	    "fullness": res_dict["fullness"],
        "fullness_date": res_dict["fullness_date"],
        "fullness_descriptions": res_dict["fullness_descriptions"],
	}
	return res_dict