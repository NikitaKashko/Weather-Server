from fastapi import FastAPI, HTTPException
import uvicorn
from models import City, Coordinates, CityWeatherRequest, User
import httpx
import aiosqlite
import asyncio
from contextlib import asynccontextmanager
import itertools
from datetime import time

async def update_cities_weather():
    """Обновляет информацию о погоде в отслеживаемых городах каждые 15 минут"""

    while True:

        async with aiosqlite.connect(DATABASE) as db:

            cursor = await db.cursor()
            await cursor.execute('SELECT city, latitude, longitude FROM weather')
            row = await cursor.fetchone()

            while row:
                coords = Coordinates(latitude=row[1], longitude=row[2])

                # Тут вопрос, на open-meteo.com есть возможность запрашивать сразу список параметров погоды для несккольких
                # координат. С одной чтобы избежать путаницы можно сразу запросить списком, с другой если вдруг один город
                # битый или их слишком много запрос может встать (кто знает сколько запросов разом open-meteo потянет)
                # Хотел бы услышать на ревью как лучше быть в такой ситуации?
                weather = await request_weather(coords)
                await db.execute(
                    'UPDATE weather SET temperature = :temperature, wind_speed = :wind_speed,'
                    f'pressure = :pressure, longitude = {coords.longitude}, latitude = {coords.latitude} WHERE city = "{row[0]}"',
                    weather
                )
                await db.commit()
                await cursor.fetchone()

            await db.commit()

        await asyncio.sleep(900)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Создаёт БД при запуске сервера и создает в ней таблицу, запускает update_cities_weather
    для отслеживания городов из БД"""

    # Создаем БД и таблички в ней или подключаемся к существующей
    async with aiosqlite.connect(DATABASE) as db:
        await db.execute(
            '''
            CREATE TABLE IF NOT EXISTS users
            (
            user_id INTEGER PRIMARY KEY,
            login TEXT NOT NULL UNIQUE
            )'''
        )
        await db.execute(
            '''
            CREATE TABLE IF NOT EXISTS weather
            (
            city TEXT NOT NULL,
            temperature TEXT,
            wind_speed TEXT,
            pressure TEXT,
            latitude REAL,
            longitude REAL,
            user_id INTEGER NOT NULL,
            CONSTRAINT pk_weather PRIMARY KEY (city, user_id),
            FOREIGN KEY (user_id) REFERENCES user(user_id)
            )'''
        )

        await db.commit()
    
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
            # Отправляем запрос на open-meteo.com.
            params_url_string = ','.join(params)
            response = await client.get(
                f'https://api.open-meteo.com/v1/forecast?latitude={coordinates.latitude}'
                f'&longitude={coordinates.longitude}&{mode}={params_url_string}&forecast_days=1'
            )

            response.raise_for_status()

        # Обрабатываем исключение если что-то с open-meteo.
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

    async with aiosqlite.connect(DATABASE) as db:
        
        cursor = await db.cursor()

        # Проверяем зарегистрирован ли пользователь по id
        await cursor.execute(f'SELECT user_id FROM users WHERE user_id = "{city.user_id}"')
        row = await cursor.fetchone()

        if row is None:
            raise HTTPException(status_code=401, detail='User with ID given is no resgistered')

        # Проверяем существует ли город в отслеживаемы
        await cursor.execute(f'SELECT city FROM weather WHERE city = "{city.name}" AND user_id = {city.user_id}')
        row = await cursor.fetchone()

        if row:
            raise HTTPException(status_code=409, detail='This city is already tracked')
        else:
            weather = await request_weather(city.coordinates)
            await db.execute(
                'INSERT INTO weather VALUES(?, ?, ?, ?, ?, ?, ?)',
                (
                    city.name,
                    weather['temperature'],
                    weather['wind_speed'],
                    weather['pressure'],
                    city.coordinates.latitude,
                    city.coordinates.longitude,
                    city.user_id
                )
            )
            await db.commit()

    
@app.get(
    '/cities/list/{user_id}',
    tags=['Погода🌤️'],
    summary='Получить список отвлеживаемых городов'
)
async def get_cities_list(user_id: int):
    """Возвращает список отслеживаемых городов по id пользователя"""

    try:
        # Базовая валидация входных данных
        user_id = int(user_id)
    except ValueError:
        raise HTTPException(status_code=409, detail='User ID is inappropriate')

    async with aiosqlite.connect(DATABASE) as db:

        # Проверка зарегистрирован ли пользователь
        cursor = await db.cursor()
        await cursor.execute(f'SELECT user_id FROM users WHERE user_id = {user_id}')
        row = await cursor.fetchone()

        if row:
            await cursor.execute(f'SELECT city FROM weather WHERE user_id = {user_id}')
            row = await cursor.fetchone()
            response = []
            while row:
                response.append(row[0])
                row = await cursor.fetchone()
            return response

        else:
            raise HTTPException(status_code=401, detail='User with ID give is not authorized')

@app.post(
    '/cities/weather',
    tags=['Погода🌤️'],
    summary='Получить заданные показатели погоды в заданном городе для конкретного пользователя'
)
async def get_city_weather(request: CityWeatherRequest):
    """Возвращает заданные параметры погоды города по его названию и id пользователя в заданное время"""
    async with aiosqlite.connect(DATABASE) as db:
        
        # Проверяем наличие записи о городе в БД
        cursor = await db.cursor()
        await cursor.execute(
            f'SELECT latitude, longitude FROM weather '
            f'WHERE city = "{request.name}" AND user_id = {request.user_id}'
        )
        row = await cursor.fetchone()

        if row:
            coords = Coordinates(latitude=row[0], longitude=row[1])
            data = await request_weather(coords, 'hourly', request.params)
        else:
            raise HTTPException(status_code=400, detail='You are unauthorized or the city is not being tracked')

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
      
@app.post(
    '/user/register',
    tags=['Пользователь'],
    summary='Регистрация нового пользователя и отправка id'
)
async def register_user(user: User):
    """Добавляет пользователя по логину в БД и высылает id в ответ"""
    async with aiosqlite.connect(DATABASE) as db:
        cursor = await db.cursor()
        await cursor.execute(f'SELECT user_id FROM users WHERE login = "{user.login}"')
        row = await cursor.fetchone()

        if row:
            return row[0]
        else:
            await cursor.execute(f'INSERT INTO users (login) VALUES ("{user.login}")')
            await db.commit()
            return cursor.lastrowid

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='127.0.0.1', port=8000)