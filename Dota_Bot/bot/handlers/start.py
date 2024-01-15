from aiogram import Bot, Router
from aiogram.types import Message
from aiogram.filters import Command
from keyboards.predict_item_kb import predict_item_keyboard

router_start = Router()

@router_start.message(Command(commands='start'))
async def get_started(message: Message, bot:Bot):
    await bot.send_message(message.from_user.id, f'Доброго времени суток \n'
                                                 f'Это бот, который содержит в себе ML модель \n\n\n', reply_markup=predict_item_keyboard)

developers = {
    "Илья Горбач": "https://t.me/paral1ax",
    "Александр Кучиев": "https://t.me/ascetto",
    "Фарид Мустафин": "https://t.me/lanarich"
}


@router_start.message(Command(commands='help'))
async def get_started(message: Message, bot:Bot):
    text = "👨🏻‍💻 Разработчики:\n\n"
    for name, link in developers.items():
        text += f"<a href='{link}'>{name}</a>\n"
    text += f"\n"
    text += f"💼 Руководитель проекта:\n\n"
    text += f"<a href='https://t.me/mariagolddd'>Мария Макарова</a>\n"
    await bot.send_message(message.from_user.id, text, reply_markup=predict_item_keyboard, disable_web_page_preview=True)
