from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from aiogram.utils.keyboard import InlineKeyboardBuilder


def place_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text=f"🌟", callback_data=f"set:1")
    kb.button(text=f"🌟🌟", callback_data=f"set:2")
    kb.button(text=f"🌟🌟🌟", callback_data=f"set:3")
    kb.button(text=f"🌟🌟🌟🌟", callback_data=f"set:4")
    kb.button(text=f"🌟🌟🌟🌟🌟", callback_data=f"set:5")
    kb.adjust(1)
    return kb.as_markup()


