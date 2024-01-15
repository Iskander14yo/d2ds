from aiogram.types import Message, CallbackQuery
from aiogram import F
from keyboards.inline import place_kb
from aiogram import Router
from utils.database import DataBase
import os
from keyboards.predict_item_kb import predict_item_keyboard

router_mark = Router()

@router_mark.message(F.text == "📌 Оценить бота 📌")
async def get_inline(message: Message):
    db = DataBase(os.getenv('DATABASE_NAME'))
    users = db.select_user_id(message.from_user.id)
    if(users):
        await message.answer(f'Вы уже оценили сервис. Спасибо!', reply_markup=predict_item_keyboard)
    else:
        await message.answer(f'Оцените меня:', reply_markup = place_kb())

@router_mark.callback_query(F.data.startswith('set'))
async def select_mark(call: CallbackQuery):
    db = DataBase(os.getenv('DATABASE_NAME'))
    mark = call.data.split(':')[1]
    users = db.select_user_id(call.from_user.id)
    if(users):
        await call.message.answer(f'Вы уже оценили сервис. Спасибо!')
    else:
        answer = f'Ваше мнение важно для нас!'
        db.add_mark(mark, call.from_user.id)
        await call.message.answer(answer, reply_markup=predict_item_keyboard)

