import pymongo
import json
from datetime import datetime
import pandas as pd
from aiogram import Bot, types, Dispatcher
from aiogram.types import ContentType
from aiogram.utils import executor
import asyncio


client = pymongo.MongoClient("mongodb://localhost:27017")
collection = client["mongo"]["employees"]

bot = Bot(token="7084193547:AAG_ZI1oxQ4SYuUHm6NebyF3p9UO-2DBC6Q")
dp = Dispatcher(bot)


async def generate_dates(dt_from, dt_upto, group_type):
    freq_map = {"hour": "h", "day": "D", "month": "MS"}
    if group_type not in freq_map:
        raise ValueError("Такая группировка не предусмотрена")
    return pd.date_range(start=dt_from, end=dt_upto, freq=freq_map[group_type])


async def get_data_from_mongodb(date_range, group_type):
    tasks = []
    for date in date_range:
        tasks.append(asyncio.create_task(get_data(date, group_type)))
    return await asyncio.gather(*tasks)


async def get_data(date, group_type):
    dt_end = {
        "hour": date + pd.Timedelta(hours=1),
        "day": date + pd.Timedelta(days=1),
        "month": date
        + pd.offsets.MonthEnd(0)
        + pd.Timedelta(hours=23, minutes=59, seconds=59),
    }[group_type]

    result = collection.aggregate(
        [
            {"$match": {"dt": {"$gte": date, "$lt": dt_end}}},
            {"$group": {"_id": None, "total": {"$sum": "$value"}}},
        ]
    )

    total = next(result, {"total": 0})["total"]
    return total, date.isoformat()


async def process_message(message):
    try:
        input_data = json.loads(message.text)
        dt_from = datetime.strptime(input_data["dt_from"], "%Y-%m-%dT%H:%M:%S")
        dt_upto = datetime.strptime(input_data["dt_upto"], "%Y-%m-%dT%H:%M:%S")
        group_type = input_data["group_type"]
        dates = await generate_dates(dt_from, dt_upto, group_type)
        data = await get_data_from_mongodb(dates, group_type)
        dataset, labels = zip(*data)
        response = {"dataset": dataset, "labels": labels}
        await message.answer(json.dumps(response))
    except Exception as e:
        await message.answer(f"Ошибка: {e}")


@dp.message_handler(content_types=ContentType.ANY)
async def message_handler(message: types.Message):
    await process_message(message)


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
