from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import asyncio, traceback
from loguru import logger
import datetime
from apps.e_com.v1.marketplaces.utils import write_data as marketplace_write_data
from apps.e_com_deviation.v1.utils import write_data as e_com_deviation_write_data
from apps.weaving.v1.utils import write_data as weaving_write_data
from apps.tkachestvo.v1.utils import write_data as tkachestvo_write_data
from apps.yarn.v1.utils import write_data as yarn_write_data
from apps.dyeing.v1.utils import write_data as dyeing_write_data
from apps.sales_plan.v1.utils import write_data as sales_plan_write_data
from apps.sales.v1.utils import write_data as sales_write_data
from apps.finishing.v1.utils import write_data as finishing_write_data
from apps.sewing.v1.utils import write_data as sewing_write_data
from apps.warp.v1.utils import write_data as warp_write_data

from apps.yarn.v1.utils import backup_data as yarn_backup_data

#==================================

async def write_data():
    tasks = [
        marketplace_write_data(),
        e_com_deviation_write_data(),
        weaving_write_data(),
        tkachestvo_write_data(),
        yarn_write_data(),
        dyeing_write_data(),
        # sales_plan_write_data(),
        sales_write_data(),
        finishing_write_data(),
        sewing_write_data(),
        warp_write_data()
    ]

    # Запускаем все задачи параллельно
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Обрабатываем результаты
    for result in results:
        if isinstance(result, Exception):
            print(traceback.format_exc())
            logger.error(f"Ошибка: {result}")
    
    
async def backup():
    await yarn_backup_data()
    
    
def init_scheduler():
    logger.info('Scheduler starting...')
    scheduler = AsyncIOScheduler()
    
    scheduler.add_job(write_data,
                      trigger=CronTrigger(minute = 55))
    scheduler.add_job(backup,
                      trigger=CronTrigger(hour=7, minute=0))
    # scheduler.add_job(marketplace_write_data,
    #                   trigger=CronTrigger(second=0))
    # scheduler.add_job(weaving_write_data,
                    #   trigger=CronTrigger(second=0))
    # scheduler.add_job(yarn_write_data,
    #                   trigger=CronTrigger(minute=0)) 
    # scheduler.add_job(dyeing_write_data,
    #                   trigger=CronTrigger(second=0))
    # scheduler.add_job(sales_plan_write_data,
    # #                   trigger=CronTrigger(minute=4))
    # scheduler.add_job(sales_write_data,
    #                    trigger=CronTrigger(second = 1))
    # scheduler.add_job(tkachestvo_write_data,
    #                    trigger=CronTrigger(second = 1))
    # scheduler.add_job(finishing_write_data,
    #                   trigger=CronTrigger(second=0))
    # scheduler.add_job(sewing_write_data,
    #                   trigger=CronTrigger(second=0))
    # scheduler.add_job(warp_write_data,
    #                   trigger=CronTrigger(second=0))
    
    scheduler.start()
    logger.info('Scheduler started!')

