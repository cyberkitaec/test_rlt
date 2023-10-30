import asyncio
import logging
import sys
import json
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command, CommandStart
import connection
import aggregate

from config import TOKEN

KEYS = ['dt_from', 'dt_upto', 'group_type']
TYPES = ['hour', 'day', 'week', 'month']
dp = Dispatcher()


@dp.message(CommandStart())
async def start(message: types.Message):
    """
    Обрабатываем /start
    """
    await message.answer(f"Hi @{message.from_user.first_name}")


@dp.message()
async def handle_message(message: types.Message):
    """
    слушаем все сообщения
    """
    # Конвертируем сообщение юзера в json-формат
    aggregate_message = []
    try:
        json_msg = json.loads(message.text)
    except:
        await message.answer("Invalid message.")
    else:
        # Проверяем на наличее нужных нам ключей
        for i in KEYS:
            if i not in json_msg.keys():
                await message.answer("Invalid keys")
                return
        if json_msg['group_type'] not in TYPES:
            await message.answer("Invalid group_type")
            return
        # конвертим дату
        json_msg['dt_from'] = datetime.fromisoformat(json_msg['dt_from'])
        json_msg['dt_upto'] = datetime.fromisoformat(json_msg['dt_upto'])

        aggregate_message = aggregate.aggregate_salary(json_msg)

    # Если длина больше 4096 символов обрезаем сообщения, т.к. внутренние ограничение тг на сообщение 4096 символов
    if len(aggregate_message) > 4096:
        await message.answer(json.dumps(aggregate_message[:4096]))
    else:
        await message.answer(json.dumps(aggregate_message))


async def main():
    bot = Bot(token=TOKEN)
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())


