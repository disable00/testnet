from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

def make_donate_keyboard(
    *,
    cryptobot_url: str = "",
    heleket_url: str = "",
    donationalerts_url: str = "",
) -> InlineKeyboardMarkup:
    """
    Клавиатура донатов без Юкассы.
    Показываем только те кнопки, у которых задан урл.
    Разкладка: по 2 в ряд (последняя строка 1, если нечётно).
    """
    b = InlineKeyboardBuilder()

    if cryptobot_url:
        b.button(text="🪙 CryptoBot", url=cryptobot_url)
    if heleket_url:
        b.button(text="🔐 Heleket (USDT TRC20)", url=heleket_url)
    if donationalerts_url:
        b.button(text="💳 DonationAlerts (СБП)", url=donationalerts_url)

    # Не считаем кнопки (в некоторых версиях .buttons — генератор).
    # Просто раскладываем по 2 в ряд; хвост сам встанет 1 кнопкой.
    b.adjust(2)

    return b.as_markup()
