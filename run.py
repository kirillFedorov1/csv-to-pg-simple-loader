import os
import json
import sys
import pandas as pd
from sqlalchemy import create_engine
import argparse as ap
import collections.abc as abc

def main(tables, chunk_num):
    """Загружает данные таблицы из CSV источников в бд.

    Args:
        tables (list): Лист названий таблиц.
        chunk_num (int): Кол-во чанков.
    """
    src_path = get_env_value('src_path')
    db_con = get_con_params()

    # Если не выбраны таблицы, берутся все из src_path.
    if not tables:
        try:
            tables = [
                f for f in os.listdir(src_path)
                if os.path.isdir(os.path.join(src_path, f))
            ]
        except FileNotFoundError:
            print(f'Не найдена директория {tables}.')
            return
        except Exception as e:
            print(f'Проблема с директорией src_path.\n{e}')
            return

    for tablename in tables:
        columns = get_table_columns_from_schema(src_path, tablename)
        if not columns:
            continue

        data = read_csv(src_path, tablename, columns, chunk_num)
        if not data:
            continue

        upload_to_db(tablename, db_con, data)

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
        Engine: Экземпляр движка соединения.
    """
    env_vars = ["db_username", "db_password", "db_host", "db_port", "db_name"]
    db_params ={var: get_env_value(var) for var in env_vars}
    db_con = create_engine(f"postgresql://{db_params['db_username']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")

    # Проверка соединения
    try:
        connection = db_con.connect()
        connection.close()
    except Exception as e:
        print(f'Возникла ошибка при подключении к базе данных.\n{e}')
        return None
    
    return db_con

def get_table_columns_from_schema(src_path, tablename):
    """Получает имена полей из файла schemas.json.

    Args:
        src_path (str): Путь к директории с schemas.json.
        tablename (str): Название таблицы.

    Returns:
        list: Список полей.
    """
    try:
        with open(os.path.join(src_path, 'schemas.json')) as json_file:
            schemas = json.load(json_file)
    except FileNotFoundError:
        print(f'Schemas.json в директории {src_path} не найден.')
        sys.exit()

    if tablename not in schemas:
        print(f'Для {tablename} нет описания в schemas.json.')
        return []

    columns = [jc['column_name'] for jc in schemas[tablename]]
    return columns

def read_csv(src_path, tablename, columns, chunk_num):
    """Читает файлы с данными таблиц и делит на чанки.

    Args:
        src_path (str): Директория с источниками.
        tablename (str): Название таблицы.
        columns (list): Поля таблицы.
        chunk_num (int): Кол-во чанков.

    Returns:
        dict: Словарь с данными таблиц, где ключ - название csv файла.
    """
    dir_path = os.path.join(src_path, tablename)
    if not os.path.exists(dir_path):
        print(f'Не найдена директория {tablename}.')
        return {}

    filenames = os.listdir(dir_path)
    if not filenames:
        print(f'Нет новых данных для {tablename}.')
        return {}

    data = {}
    for filename in filenames:
        full_path = os.path.join(src_path, tablename, filename)
        try:
            data[filename] = pd.read_csv(
                full_path,
                names=columns,
                chunksize=chunk_num
            )

            # Проверка содержимого файла, если он делится на чанки
            if isinstance(data[filename], abc.Iterator):
                valid = validate_file(tablename, filename, data[filename])
                # Переинициализация итератора
                data[filename] = pd.read_csv(
                    full_path,
                    names=columns,
                    chunksize=chunk_num
                ) if valid else None
        except Exception as e:
            print(f'Проблема с файлом {filename} (таблица {tablename}).\n{e}.')
            data[filename] = None

    return data

def validate_file(tablename, filename, data):
    """Проверка содержимого файла, если он разделен на чанки.

    Args:
        tablename (str): Название таблицы.
        filename (str): Название файла.
        data (Iterator): Итератор чанков.

    Returns:
        bool: False, если проблема с файлом.
    """
    try:
        for chunk in data:
            pass
    except pd.errors.ParserError as e:
        print(f'Проблема с файлом {filename} (таблица {tablename}).\n{e}')
        return False
    return True

def upload_to_db(tablename, db_con, data):
    """Загрузка данных таблиц в бд.

    Args:
        tablename (str): Название таблицы.
        db_con (Engine): Движок подключения.
        data (dict): Словарь с данными таблиц, где ключ - название csv файла.
    """
    for filename, item in data.items():
        # Если не было разделения на чанки
        if isinstance(item, pd.DataFrame):
            chunks = [item]
        elif isinstance(item, abc.Iterator):
            chunks = item
        else:
            continue

        for i, chunk in enumerate(chunks):
            try:
                chunk.to_sql(tablename, db_con, if_exists='append', index=False)
                print(f'{tablename}, {filename}, чанк {i} загружен в бд.')
            except Exception as e:
                print(f'Ошибка при загрузке {tablename}, {filename}, чанк {i}.\n{e}')

if __name__ == '__main__':
    parser = ap.ArgumentParser(description='Загрузка табличных данных CSV в бд.')

    parser.add_argument(
        '-t', '--tables',
        type=str,
        help='Список таблиц, разделенных запятыми'
    )
    parser.add_argument(
        '-ch', '--chunks',
        type=int,
        help='Количество чанков для последовательной загрузки в бд'
    )

    args = parser.parse_args()

    if args.tables:
        tables = args.tables.split(', ')
    else:
        tables = []
        print('Обрабатываю все таблицы.')

    if args.chunks:
        chunk_num = args.chunks
    else:
        chunk_num = None
    
    main(tables, chunk_num)