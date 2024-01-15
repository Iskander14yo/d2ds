from aiogram.types import Message, BufferedInputFile
from aiogram.fsm.context import FSMContext
from aiogram import Router, F, Bot
from state.prediction import PredictionState
import pickle
import pandas as pd
import io
import json
import catboost
from keyboards.predict_item_kb import predict_item_keyboard


def load_model():
    with open('catboosth5.h5', 'rb') as file:
        model = pickle.load(file)
    return model

columns_for_drop = ['_id', 'dire_POSITION_2_id', 'radiant_POSITION_4_id', 'radiant_POSITION_1_id',
                    'radiant_POSITION_2_position', 'startDateTime', 'radiant_POSITION_2_id', 'radiant_POSITION_5_id',
                    'dire_POSITION_2_position', 'radiant_POSITION_3_position', 'dire_POSITION_5_id',
                    'dire_POSITION_4_position', 'radiant_POSITION_5_position', 'dire_POSITION_1_id',
                    'durationSeconds', 'radiant_POSITION_4_position', 'dire_POSITION_4_id',
                    'dire_POSITION_1_position', 'radiant_POSITION_1_position', 'radiant_POSITION_3_id',
                    'dire_POSITION_3_id', 'id', 'dire_POSITION_3_position', 'gameMode', 'dire_POSITION_5_position',
                    ]

columns_with_leaver_status = ['dire_POSITION_4_leaverStatus', 'radiant_POSITION_3_leaverStatus',
                              'radiant_POSITION_2_leaverStatus', 'dire_POSITION_5_leaverStatus',
                              'dire_POSITION_3_leaverStatus', 'dire_POSITION_2_leaverStatus',
                              'dire_POSITION_1_leaverStatus', 'radiant_POSITION_4_leaverStatus',
                              'radiant_POSITION_5_leaverStatus', 'radiant_POSITION_1_leaverStatus']

columns_true_false_convert = ['dire_POSITION_5_isRadiant', 'dire_POSITION_4_intentionalFeeding',
                              'radiant_POSITION_5_isRadiant', 'dire_POSITION_5_intentionalFeeding',
                              'dire_POSITION_1_isRadiant', 'didRadiantWin', 'radiant_POSITION_3_intentionalFeeding',
                              'dire_POSITION_3_intentionalFeeding', 'radiant_POSITION_1_isRadiant',
                              'radiant_POSITION_4_isRadiant', 'radiant_POSITION_2_isRadiant',
                              'dire_POSITION_4_isRadiant', 'dire_POSITION_3_isRadiant',
                              'radiant_POSITION_3_isRadiant', 'radiant_POSITION_1_intentionalFeeding',
                              'radiant_POSITION_4_intentionalFeeding', 'radiant_POSITION_2_intentionalFeeding',
                              'dire_POSITION_2_isRadiant', 'dire_POSITION_1_intentionalFeeding',
                              'dire_POSITION_2_intentionalFeeding', 'radiant_POSITION_5_intentionalFeeding']

leaver_status_map = {'NONE': 2, 'DISCONNECTED': 3, 'AFK': 4, 'DISCONNECTED_TOO_LONG': 5, 'ABANDONED': 6}


def preprocess_data(dataframe):
    df_dropped = dataframe.drop(columns_for_drop, axis=1)
    df_dropped[columns_true_false_convert] = df_dropped[columns_true_false_convert].astype(int)
    df_dropped = df_dropped.fillna(9999)
    df_dropped[columns_with_leaver_status] = (df_dropped[columns_with_leaver_status] != 'NONE').astype(int)
    df_dropped = df_dropped.drop('didRadiantWin', axis=1)
    return df_dropped


router = Router()

@router.message(F.text == "🔮 Предсказания для игры 🔮")
async def start_predict_items(message: Message, state: FSMContext):
    await message.answer('Давайте поминутно предскажем вероятность победы. \n'
                         'Для этого загрузите файл формата <b>csv</b> весом не более <b>20 МБ</b> \n'
                         )
    await state.set_state(PredictionState.predItems)

@router.message(PredictionState.predItems)
async def predict_items(message: Message, bot: Bot):
    document = await bot.download(message.document)
    data = pd.read_csv(document)
    data_old = data.copy()
    trained_model = load_model()
    processed_data = preprocess_data(data_old)
    prediction = trained_model.predict(processed_data)
    # df_final = pd.concat([data_old, pd.DataFrame(prediction, columns=['selling_price_pred'])], axis = 1)
    # csv_data = df_final.to_csv()
    # pred_file = BufferedInputFile(io.BytesIO(csv_data.encode()).getvalue(), filename="Predictions.txt")
    await message.answer(f'Данные предсказаны!\n'
                         f'Скачайте новый файл, в нем появился столбец selling_price_pred', reply_markup= predict_item_keyboard)
    # await bot.send_document(message.chat.id, pred_file)
