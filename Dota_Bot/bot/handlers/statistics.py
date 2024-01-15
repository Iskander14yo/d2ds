import os

from aiogram.types import Message, FSInputFile
from aiogram import Router, F
from utils.database import DataBase
import pandas as pd
import matplotlib.pyplot as plt
from keyboards.predict_item_kb import predict_item_keyboard


def Graph(data):
    df = pd.DataFrame(data, columns=['user_mark'])
    df['user_mark'] = df['user_mark'].astype(int)

    rating_counts = df['user_mark'].value_counts()
    labels = rating_counts.index
    colors = ['lightcoral', 'lightblue', 'lightgreen', 'lightyellow', 'lightpink']

    plt.pie(rating_counts, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
    plt.axis('equal')
    plt.savefig('rating_pie_chart.png')
    plt.close()






router_stat = Router()

@router_stat.message(F.text == "📈 Статистика оценок 📈")
async def statistic(message: Message):
    db = DataBase(os.getenv('DATABASE_NAME'))
    marks = db.select_marks()
    Graph(marks)
    await message.answer('Не судите строго 🙏', reply_markup=predict_item_keyboard)
    file_path = 'rating_pie_chart.png'
    image_from_pc = FSInputFile(file_path)
    await message.answer_photo(image_from_pc)



