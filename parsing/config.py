"""
Разделение рангов
Илья - Immortal
Фарид - Легенды
Саша - Эншинт
Искандер - Дивайн
"""
import datetime
from enum import Enum


class RankSpread(Enum):
    LEGEND_LOW = 50
    LEGEND_TOP = 55
    ANCIENT_LOW = 60
    ANCIENT_TOP = 65
    DIVINE_LOW = 70
    DIVINE_TOP = 75
    IMMORTAL_LOW = 80
    IMMORTAL_TOP = 85


STRATZ_URL = "https://api.stratz.com/graphql"
OPENDOTA_URL = "https://api.opendota.com/api/publicMatches"

# Токен стратца (вставить свой)
LOCAL_STRATZ_TOKEN = "YOUR_API_TOKEN"
# Нижний предел ранга (вставить свой)
LOCAL_SPREAD_BOT = RankSpread.IMMORTAL_LOW.value
# Верхний предел ранга (вставить свой)
LOCAL_SPREAD_TOP = RankSpread.IMMORTAL_TOP.value

# Локальное время
UTC_PLUS_3 = datetime.timezone(datetime.timedelta(hours=3))
# Количество запросов в OpenDota (default = 99)
REMAIN_OPENDOTA_REQUESTS = 99
# Количество запросов в Stratz (default = 9900)
REMAIN_STRATZ_REQUESTS = 9900
# Количество одновременных запросов в stratz (default = 5)
BATCH_SIZE = 15

# Ставьте тру, если запускаете первый раз.
# После выполнения скрипта первый раз, ставьте false.
FIRST_RUN = False
# Id для первого матча. Залезть в dotabuff
# Найдите id какого нить матча, который был вчера и вставьте сюда
# Лучше искать по своему рангу, потому что не гарантирую что ранг другого матча прокатит
LESS_THEN_MATCH = 7451652617
