from pydantic import BaseModel, Field, field_validator
from datetime import time
from typing import List, ClassVar

class User(BaseModel):
    login: str = Field(max_length=300)

class Coordinates(BaseModel):
    """Класс для валидации данных о координатах"""

    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)

class City(BaseModel):
    """Класc для ваилдации данных о городах"""

    name: str = Field(max_length=300)
    coordinates: Coordinates
    user_id: int = Field(gt=0)

class CityWeatherRequest(BaseModel):
    """Класс для валидации данных для запроса на open-meteo.com"""

    name: str = Field(max_length=300)
    request_time: time
    user_id: int = Field(gt=0)
    params: List[str]

    allowed_params: ClassVar = ('temperature_2m', 'wind_speed_10m', 'precipitation', 'relative_humidity_2m')

    # Валидатор для проверки списка получаемых параметров, должны быть из allowed_params. ^
    @field_validator('params', mode='after')  
    @classmethod
    def params_validation(cls, parameters):
        """Проверяет на валидность список параметров"""
        if len(parameters) > len(set(parameters)):
            raise ValueError(f'Parameters should not be duplicated')
        for param in parameters:
            if param not in cls.allowed_params:
                raise ValueError(f'Parameters must be as follows: {cls.allowed_params}')
        return parameters

