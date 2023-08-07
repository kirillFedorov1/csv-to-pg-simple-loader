import os
import json
import sys
import pandas as pd
from sqlalchemy import create_engine
import argparse as ap
import collections.abc as abc
import multiprocessing as mp
from functools import partial

def main(tablenames, chunk_num):
    """Загружает данные таблицы из CSV источников в бд.

    Args:
        tables (list): Лист названий таблиц.
        chunk_num (int): Кол-во чанков.
    """
    src_path = get_env_value('src_path')
    db_con = get_con_params()

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

        upload_to_db(tablename, db_con, data)

def json_load(src_path):
    """Читает schemas.json.

    Args:
        src_path (str): Рабочая директория.
    """
    try:
        with open(os.path.join(src_path, 'schemas.json')) as json_file:
            return json.load(json_file)
    except FileNotFoundError:
        print(f'Schemas.json в директории {src_path} не найден.')
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
        print(f'Проблема в директории src_path.\n{e}')
        sys.exit()

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
        print(f'Не задана переменная окружения \'{key}\'.')
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
        print(f'Возникла ошибка при подключении к базе данных.\n{e}')
        sys.exit()
    finally:
        if connection:
            connection.close()
    
    return db_con

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
        print(f'Для {tablename} нет описания в schemas.json.')
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
        print(f'Не найдена директория {tablename}.')
        return {}

    filenames = [
        f
        for f in os.listdir(dir_path)
        if os.path.isfile(os.path.join(dir_path, f))
    ]
    if not filenames:
        print(f'Нет новых данных для {tablename}.')
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

            # Проверка содержимого файла, если он делится на чанки
            if isinstance(data[filename], abc.Iterator):
                valid = validate_chunks(tablename, filename, data[filename])
                # Переинициализация итератора
                data[filename] = pd.read_csv(
                    full_path,
                    names=columns,
                    chunksize=chunk_num,
                ) if valid else None
        except Exception as e:
            print(f'Проблема с файлом {filename} таблицы {tablename}:\n{e}')
            data[filename] = None

    data = {k: v for k, v in data.items() if v is not None}
    return data

def validate_chunks(tablename, filename, data):
    """Проверка содержимого файла, если он разделен на чанки.

    Args:
        tablename (str): Название таблицы.
        filename (str): Название файла.
        data (Iterator): Итератор чанков.

    Returns:
        bool: False, если проблема с файлом; True, если файл - ок.
    """
    try:
        for chunk in data:
            pass
    except Exception as e:
        print(f'Проблема с файлом {filename} таблицы {tablename}:\n{e}')
        return False
    return True

def upload_to_db(tablename, db_con, data):
    """Выбор режима загрузки в бд.

    Args:
        tablename (str): Название таблицы.
        db_con (str): Адрес подключения.
        data (dict): Словарь с данными таблиц, где ключ - название csv файла.
    """
    cpu_count = os.cpu_count() or 1

    results = []
    for filename, item in data.items():
        # Если нет разделения на чанки
        if isinstance(item, pd.DataFrame):
            results.append(to_sql(tablename, filename, db_con, (0, item)))

        # Eсли есть разделение на чанки и кол-во ядер равно 1
        elif isinstance(item, abc.Iterator) and cpu_count == 1:
            for i, data in enumerate(item):
                results.append(to_sql(tablename, filename, db_con, (i, data)))

        # Если есть разделение на чанки и используем многопроцессорность
        elif isinstance(item, abc.Iterator):
            with mp.Pool(cpu_count) as pool:
                partial_to_sql = partial(to_sql, tablename, filename, db_con)
                results.extend(pool.map(partial_to_sql, enumerate(item)))

        else:
            print(f'Проблема с файлом {filename} таблицы {tablename}.')
            continue

        if all(results):
            print(f'Файл {filename} таблицы {tablename} успешно загружен.')
        results = []

def to_sql(tablename, filename, db_con, chunk):
    """Загрузка данных таблиц в бд.

    Args:
        tablename (str): Название таблицы.
        filename (str): Название файла.
        db_con (str): Адрес подключения.
        chunk (int, DataFrame): Кортеж, датафрейм и его индекс.
    """
    i, data = chunk
    # Пересоздаем движок из-за многопроцессорности
    my_engine = create_engine(db_con)
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
        print(f"Ошибка при загрузке {tablename}, {filename}, чанк {i}.")
        # Чтобы не выводить в консоль INSERT
        print(str(e).split('\n')[0])
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
        print('Обрабатываю все таблицы.')

    chunk_num = args.chunks
    
    main(tables, chunk_num)