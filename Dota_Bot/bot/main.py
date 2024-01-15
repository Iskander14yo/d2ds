import asyncio
import logging

from aiogram import Bot, Dispatcher

from dotenv import load_dotenv
import os
from utils.commands import set_commands
from handlers import predict_items, user_mark, statistics, start
from state.prediction import PredictionState




load_dotenv()

token = os.getenv('BOT_TOKEN')

logging.basicConfig(level=logging.INFO)

bot = Bot(token=token, parse_mode='HTML')
dp = Dispatcher()


dp.include_routers(predict_items.router, user_mark.router_mark, statistics.router_stat, start.router_start)


async def start():
    await set_commands(bot)
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(start())
