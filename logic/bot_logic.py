from os.path import dirname, abspath

import pandas as pd
from asgiref.sync import sync_to_async

from logic.db_utils.db_additional_utils import create_task_db_logic, load_tasks_db_logic

base_dir = dirname(abspath(__file__))


@sync_to_async
def create_task_logic(logger, config, telegram_id, task_type):
    try:
        task_info_df = pd.DataFrame([{'telegram_id': telegram_id, 'task_type': task_type}])
        create_task_db_logic(logger, config, task_info_df)
        logger.info(f'Задача - create_task_logic для tg - {telegram_id} завершилась созданием задачи - {task_type}')
        return True

    except Exception as e:
        logger.warning(f'Задача - create_task_logic для tg - {telegram_id} завершилась ошибкой {e}')
        raise e
        return False


@sync_to_async
def load_tasks_logic(logger, config, telegram_id):
    try:
        task_info_df = load_tasks_db_logic(logger, config, base_dir, telegram_id)
        logger.info(f'Задача - load_tasks_logic для tg - {telegram_id} завершилась загрузкой задач - {task_info_df}')
        return task_info_df

    except Exception as e:
        logger.warning(f'Задача - load_tasks_logic для tg - {telegram_id} завершилась ошибкой {e}')
        logger.warning(f'Задача - create_task_logic для tg - {telegram_id} завершилась ошибкой {e}')
        raise e
        return False
