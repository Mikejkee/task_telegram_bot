from .connector_utils import load_data_from_bd, load_data_in_db


def create_task_db_logic(logger, config, task_info_df):
    load_data_in_db(task_info_df, logger, config, 'public', 'tasks')
    return True


def load_tasks_db_logic(logger, config, base_dir, telegram_id):
    tasks = load_data_from_bd(logger, config, 'select_tasks_tg_id.sql', base_dir, 'public', 'tasks',
                              params_values={'telegram_id': str(telegram_id)}, expanding=False)

    if not tasks.empty:
        return tasks.task_type.to_list()
    else:
        return False
