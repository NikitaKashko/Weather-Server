from fastapi import FastAPI, HTTPException
import uvicorn
from models import City, Coordinates, CityWeatherRequest
import httpx
import aiosqlite
import asyncio
from contextlib import asynccontextmanager
import itertools
from datetime import time

# Хранит отслеживаемые города и их координаты.
cities = {}

async def update_cities_weather():
    """Обновляет информацию о погоде в отслеживаемых городах каждые 15 минут"""

    while True:

        async with aiosqlite.connect(DATABASE) as db:
            for city, coordinates in cities.items():
                weather = await request_weather(coordinates)
                await db.execute(
                    'UPDATE weather SET temperature = :temperature, wind_speed = :wind_speed,'
                    f'pressure = :pressure, longitude = {city.longitude}, latitude = {city.latitude} WHERE city = "{city}"',
                    weather
                )
                await db.commit()

            await db.commit()

        await asyncio.sleep(900)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Создаёт БД при запуске сервера и создает в ней таблицу, запускает update_cities_weather
    заполняет словарь cities для отслеживания городов из БД"""

    async with aiosqlite.connect(DATABASE) as db:
        await db.execute(
            '''CREATE TABLE IF NOT EXISTS weather
            (
            city TEXT PRIMARY KEY NOT NULL,
            temperature TEXT, 
            wind_speed TEXT, 
            pressure TEXT,
            latitude REAL,
            longitude REAL
            )'''
        )

        await db.commit()
        cursor = await db.cursor()
        await cursor.execute('SELECT city, latitude, longitude FROM weather')

        # Если в БД были записи, переносим их в словарь cities.
        while True:
            row = await cursor.fetchone()
            if row is None:
                break
            cities[row[0]] = Coordinates(latitude=float(row[1]), longitude=float(row[2]))
    
    task = asyncio.create_task(update_cities_weather())
    yield

    task.cancel()

# Инициализируем приложение.
app = FastAPI(lifespan=lifespan)
DATABASE = 'weather.db'


async def request_weather(
        coordinates: Coordinates,
        mode='current', # Параметр для выбора режима запроса: текущая погода или почасовой прогноз на день.
        params=['temperature_2m', 'surface_pressure', 'wind_speed_10m'] # Запрашиваемые параметры погоды.
    ):
    """Отправляет запрос по координатам нужных параметров на open-meteo.com"""

    async with httpx.AsyncClient() as client:

        try:
            params_url_string = ','.join(params)
            response = await client.get(
                f'https://api.open-meteo.com/v1/forecast?latitude={coordinates.latitude}'
                f'&longitude={coordinates.longitude}&{mode}={params_url_string}&forecast_days=1'
            )

            response.raise_for_status()

        except httpx.HTTPStatusError:
            raise HTTPException(status_code=523, detail='The open-meteo.com is unreachable')

        data = response.json()[mode]

        # Разные ответы - из-зы разного характераответов open-mete.com.
        if mode == 'current':
            weather = {
                'temperature': data['temperature_2m'],
                'wind_speed': data['wind_speed_10m'],
                'pressure': data['surface_pressure']
            }
            return weather

        else:
            return data

@app.post(
    '/weather/current',
    tags=['Погода🌤️'],
    summary=(
        'Получить данные о температуре, скорости ветра и давлении'
        'по заданным координатам'
    )
)
async def get_current_weather(coordinates: Coordinates):
    """Возвращает темепратуру, давление и скорость ветра по координатам"""

    weather = await request_weather(coordinates)
    return weather

@app.post(
    '/cities/add',
    tags=['Погода🌤️'],
    summary='Добавить город в список отслеживаемых городов'
)
async def add_city(city: City):
    """Добавляет город в список отслеживаемых"""

    # Проверяем существует ли город в отслеживаемых
    if city.name not in cities:
        cities[city.name] = city.coordinates
        weather = await request_weather(city.coordinates)

        async with aiosqlite.connect(DATABASE) as db:
            await db.execute(
                'INSERT INTO weather VALUES(?, ?, ?, ?, ?, ?)',
                (
                    city.name,
                    weather['temperature'],
                    weather['wind_speed'],
                    weather['pressure'],
                    city.coordinates.latitude,
                    city.coordinates.longitude
                )
            )
            await db.commit()

    else:
        raise HTTPException(status_code=409, detail='This city already exists')
    
@app.get(
    '/cities/list',
    tags=['Погода🌤️'],
    summary='Получить список отвлеживаемых городов'
)
async def get_cities_list():
    """Возвращает список отслеживаемых городов"""

    return list(cities.keys())

@app.post(
    '/cities/weather',
    tags=['Погода🌤️'],
    summary='Получить заданные показатели погоды в заданном городе'
)
async def get_city_weather(request: CityWeatherRequest):
    """Возвращает заданные параметры погоды города по его названию в заданное время"""

    if request.name not in cities:
        raise HTTPException(status_code=409, detail='This city is untracked')
    else:
        data = await request_weather(cities[request.name], 'hourly', request.params)

    # Т.к. open-meteo.com не даёт данные по времени, ищем ближайшие параметры из почасовых прогнозов
    if request.request_time.minute > 30:
        hour = request.request_time.hour + 1
    else:
        hour = request.request_time.hour

    response = {}

    for elem in data.keys():
        if elem != 'time':
            response[elem] = data[elem][hour]
    
    return response
      

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='127.0.0.1', port=8000)