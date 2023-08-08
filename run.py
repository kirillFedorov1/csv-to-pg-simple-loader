import os
import json
import sys
import pandas as pd
from sqlalchemy import create_engine
import argparse as ap
import collections.abc as abc
from functools import partial
import concurrent.futures as cf
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def main(tablenames, chunk_num):
    """Загружает данные таблицы из CSV источников в бд.

    Args:
        tables (list): Лист названий таблиц.
        chunk_num (int): Кол-во чанков.
    """
    src_path = get_env_value('src_path')
    my_engine = create_engine(get_con_params())

    schemas = json_load(src_path)

    # Если не выбраны таблицы, берутся все из src_path
    if not tablenames:
        tablenames = get_all_tablenames(src_path)

    for tablename in tablenames:
        columns = get_table_columns_from_schema(src_path, tablename, schemas)
        if not columns:
            continue

        data = read_csv(src_path, tablename, columns, chunk_num)
        if not data:
            continue

        upload_to_db(tablename, my_engine, data)

def get_env_value(key):
    """Получает значение переменной окружения.

    Args:
        key (str): Ключ переменной окружения.

    Returns:
        str: Значение переменной окружения.
    """
    try:
        return os.environ[key]
    except KeyError:
        logger.error(f'Не задана переменная окружения \'{key}\'.')
        sys.exit()

def get_con_params():
    """Получает параметры подключения и проверяет соединение.

    Returns:
        str: Адрес подключения к базе данных.
    """
    env_vars = ["db_username", "db_password", "db_host", "db_port", "db_name"]
    db_params ={var: get_env_value(var) for var in env_vars}
    db_con = f"postgresql://{db_params['db_username']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}"

    # Проверка соединения
    connection = None
    try:
        my_engine = create_engine(db_con)
        connection = my_engine.connect()
    except Exception as e:
        logger.error(f'Возникла ошибка при подключении к базе данных.\n{e}')
        sys.exit()
    finally:
        if connection:
            connection.close()
    
    return db_con

def json_load(src_path):
    """Читает schemas.json.

    Args:
        src_path (str): Рабочая директория.
    """
    try:
        with open(os.path.join(src_path, 'schemas.json')) as json_file:
            return json.load(json_file)
    except FileNotFoundError:
        logger.error(f'Schemas.json в директории {src_path} не найден.')
        sys.exit()

def get_all_tablenames(src_path):
    """Список всех таблиц в рабочей директории.

    Args:
        src_path (str): Рабочая директория.
    """
    try:
        return [
            f
            for f in os.listdir(src_path)
            if os.path.isdir(os.path.join(src_path, f))
        ]
    except Exception as e:
        logger.error(f'Проблема в директории src_path.\n{e}')
        sys.exit()

def get_table_columns_from_schema(src_path, tablename, schemas):
    """Получает имена полей из файла schemas.json.

    Args:
        src_path (str): Путь к директории с schemas.json.
        tablename (str): Название таблицы.

    Returns:
        list: Список полей.
    """
    table_schema = schemas.get(tablename, [])
    if not table_schema:
        logger.error(f'Для {tablename} нет описания в schemas.json.')
        return []

    columns = [jc['column_name'] for jc in table_schema]
    return columns

def read_csv(src_path, tablename, columns, chunk_num):
    """Читает файлы с данными таблиц и делит на чанки.

    Args:
        src_path (str): Директория с источниками.
        tablename (str): Название таблицы.
        columns (list): Поля таблицы.
        chunk_num (int): Кол-во чанков.

    Returns:
        dict: Словарь с данными и кодировкой, где ключ - название csv файла.
    """
    dir_path = os.path.join(src_path, tablename)
    if not os.path.exists(dir_path):
        logger.error(f'Не найдена директория {tablename}.')
        return {}

    filenames = [
        f
        for f in os.listdir(dir_path)
        if os.path.isfile(os.path.join(dir_path, f))
    ]
    if not filenames:
        logger.info(f'Нет новых данных для {tablename}.')
        return {}

    data = {}
    for filename in filenames:
        full_path = os.path.join(dir_path, filename)
            
        try:
            # Чтение файла
            data[filename] = pd.read_csv(
                full_path,
                names=columns,
                chunksize=chunk_num,
            )
        except Exception as e:
            logger.error(f'Проблема с файлом {filename} таблицы {tablename}:\n{e}')
            data[filename] = None

    data = {k: v for k, v in data.items() if v is not None}
    return data

def upload_to_db(tablename, my_engine, data):
    """Выбор режима загрузки в бд.

    Args:
        tablename (str): Название таблицы.
        my_engine (Engine): Движок подключения к бд.
        data (dict): Словарь с данными таблиц, где ключ - название csv файла.
    """
    for filename, item in data.items():
        # Если нет разделения на чанки
        if isinstance(item, pd.DataFrame):
            to_sql(tablename, filename, my_engine, item)
        # Eсли есть разделение на чанки, использую многопоточность
        elif isinstance(item, abc.Iterator):
            try:
                with cf.ThreadPoolExecutor() as executor:
                    partial_to_sql = partial(to_sql, tablename, filename, my_engine)
                    future_results = {
                        executor.submit(partial_to_sql, (i, chunk))
                        for i, chunk in enumerate(item)
                    }

                for future in cf.as_completed(future_results):
                    result = future.result()
                    if not result:
                        break
            except Exception as e:
                logger.error(f"Проблема с файлом {filename}, таблица {tablename}:\n{e}")

    logger.info(f'Данные таблицы {tablename} обработаны.')

def to_sql(tablename, filename, my_engine, chunk):
    """Загрузка данных таблиц в бд.

    Args:
        tablename (str): Название таблицы.
        filename (str): Название файла.
        my_engine (Engine): Движок подключения к бд.
        chunk (int, DataFrame): Кортеж, датафрейм и его индекс.

    Returns:
        bool: Возвращает True, если загрузка успешна, иначе False.
    """
    if isinstance(chunk, tuple):
        i, data = chunk
    else:
        i = 0
        data = chunk
    try:
        data.to_sql(
            tablename,
            my_engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        return True
    except Exception as e:
        # Чтобы не выводить в консоль весь INSERT
        err_msg = str(e).split('\n')[0]
        logger.error(
            f"Ошибка при загрузке"
            f"{tablename},"
            f"{filename},"
            f", чанк {i}."
            f"\n{err_msg}"
        )
        return False

if __name__ == '__main__':
    parser = ap.ArgumentParser(description='Загрузка табличных данных CSV в бд.')

    parser.add_argument(
        '-t', '--tables',
        type=str,
        default='',
        help='Список таблиц, разделенных запятыми'
    )
    parser.add_argument(
        '-ch', '--chunks',
        type=int,
        default=None,
        help='Количество чанков для последовательной загрузки в бд'
    )

    args = parser.parse_args()
    
    tables = args.tables.split(',') if args.tables else []
    if not tables:
        logger.info('Обрабатываю все таблицы.')

    chunk_num = args.chunks
    
    main(tables, chunk_num)