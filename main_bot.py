import datetime
import os
import logging

import asyncio
from asgiref.sync import sync_to_async
from dotenv import load_dotenv
from aiogram import F, Bot, Dispatcher, Router
from aiogram.types import Message, KeyboardButton, ReplyKeyboardMarkup
from aiogram.filters.command import Command
from aiogram.client.bot import DefaultBotProperties
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.storage.redis import RedisStorage

from logic.bot_logic import create_task_logic, load_tasks_logic

# Логирование
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

py_handler = logging.FileHandler(f"logs/{__name__}_{datetime.datetime.now().timestamp()}.log", mode='w+',
                                 encoding='utf-8')
py_formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

py_handler.setFormatter(py_formatter)
logger.addHandler(py_handler)

load_dotenv()
TOKEN = os.environ.get('TELEGRAM_TOKEN')
config = {
    'psql_login': os.environ.get('SQL_USER'),
    'psql_password': os.environ.get('SQL_PASSWORD'),
    'psql_hostname': os.environ.get('SQL_HOST'),
    'psql_port': os.environ.get('SQL_PORT'),
    'psql_name_bd': os.environ.get('SQL_DATABASE'),
    'psql_conn_type': 'postgres'
}
router = Router()
# storage = RedisStorage.from_url(os.getenv('CELERY_BACKEND'))
storage = MemoryStorage()


@sync_to_async
def start_menu_buttons():
    buttons = [
        [
            KeyboardButton(text='/add'),
            KeyboardButton(text='/tsk'),
        ],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


class RequestState(StatesGroup):
    request_task_type = State()


@router.message(F.text, Command("start"))
async def start(message: Message, state: FSMContext):
    await state.clear()

    keyboard = await start_menu_buttons()
    await message.answer('Напишите команду. \n'
                         '/add - Создание задачи. \n'
                         '/tsk - Просмотр списка задач. \n',
                         parse_mode='HTML', reply_markup=keyboard)


@router.message(F.text, Command("add"))
async def add_task(message: Message, state: FSMContext):
    await message.reply("Введите текст задачи.")
    await state.set_state(RequestState.request_task_type)


@router.message(RequestState.request_task_type)
async def request_task(message: Message, state: FSMContext):
    telegram_id = message.from_user.id

    await create_task_logic(logger, config, telegram_id=telegram_id, task_type=message.text)

    await message.reply("Задача добавлена!")
    await state.clear()


@router.message(F.text, Command("tsk"))
async def list_tasks(message: Message, state: FSMContext):
    await state.clear()
    telegram_id = message.from_user.id

    tasks = await load_tasks_logic(logger, config, telegram_id=telegram_id)

    if tasks:
        await message.reply("\n".join(tasks))
    else:
        await message.reply("Список задач пуст.")


async def main():
    bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))

    dp = Dispatcher(storage=storage)
    dp.include_router(router)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
