import asyncio
import aiomysql

import secure

from secure import log
from secure import PSql


async def create_connection():
    pool = await aiomysql.create_pool(
        host=PSql.host,
        user=PSql.user,
        password=PSql.password,
        db=PSql.db_name,
        connect_timeout=60,
        # maxsize=10,
        # minsize=1,
        charset=PSql.charset,
        autocommit=True
    )
    return pool


async def write_sign_cards(pool, url, real_id, period, dogovor, dogovor_date, date_publish, type_card,
                           period_start, period_end, comments, done):
    try:
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(f"""INSERT INTO cards (url, done, real_id, period, dogovor, dogovor_date,
                date_publish, type, period_start, period_end, comments) VALUES
                    ('{url}', '{done}', '{real_id}', '{period}', '{dogovor}', '{dogovor_date}', '{date_publish}',
                    '{type_card}', '{period_start}', '{period_end}', '{comments}');"""
                                     )
    except Exception as ex:
        log.write_log("db_sql_write_sign_cards", ex)
        print("db_sql_write_sign_cards", ex)
        pass


async def write_change_cards(pool, url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url, dogovor_date,
                             date_publish, type_card, period_start, period_end, date_add, comments, done):
    try:
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"""INSERT INTO cards_change (url, done, real_id, period, dogovor, dogovor_main_real_id,
                                    dogovor_main_url, dogovor_date, date_publish, type, period_start, period_end,
                                    date_add, comments, main_card) VALUES
                                    ('{url}', '{done}', '{real_id}', '{period}', '{dogovor}', '{dogovor_main_real_id}',
                                    '{dogovor_main_url}', '{dogovor_date}', '{date_publish}', '{type_card}',
                                    '{period_start}', '{period_end}', '{date_add}', '{comments}', '');"""
                )
    except Exception as ex:
        log.write_log("db_sql_write_change_cards", ex)
        print("db_sql_write_change_cards", ex)
        pass


async def write_stop_cards(pool, url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url, reason_stop,
                           dogovor_date, dogovor_stop_date, date_publish, comments, type_card, done):
    try:
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"""INSERT INTO cards_stop (url, done, real_id, period, dogovor, dogovor_main_real_id,
                    dogovor_main_url, reason_stop, dogovor_date, dogovor_stop_date, date_publish, comments, type) VALUES
                    ('{url}', '{done}', '{real_id}', '{period}', '{dogovor}', '{dogovor_main_real_id}',
                    '{dogovor_main_url}', '{reason_stop}', '{dogovor_date}', '{dogovor_stop_date}', '{date_publish}',
                    '{comments}', '{type_card}');"""
                )
    except Exception as ex:
        log.write_log("db_sql_write_stop_cards", ex)
        print("db_sql_write_stop_cards", ex)
        pass


async def write_lessees(pool, url, name, inn, ogrn, table):
    try:
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"""INSERT INTO {table} (card_id, name, inn, ogrn, nomreg) VALUES 
                        ('{url}', '{name}', '{inn}', '{ogrn}', '');"""
                )
    except Exception as ex:
        log.write_log("db_sql_write_lessees", ex)
        print("db_sql_write_lessees", ex)
        pass


async def write_lessors(pool, url, name, inn, ogrn, table):
    try:
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"""INSERT INTO {table} (card_id, name, inn, ogrn) VALUES 
                        ('{url}', '{name}', '{inn}', '{ogrn}');"""
                )
    except Exception as ex:
        log.write_log("db_sql_write_lessors", ex)
        print("db_sql_write_lessors", ex)
        pass


async def write_objects(pool, url, object_guid, object_name, object_class, object_description, object_total, table):
    try:
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"""INSERT INTO {table} (card_id, guid, name, class, description, total, category, category_word_w,
                    type, type_word, marka, model) VALUES 
                        ('{url}', '{object_guid}', '{object_name}', '{object_class}', '{object_description}',
                        '{object_total}', '', '', '', '', '', '');"""
                )
    except Exception as ex:
        log.write_log("db_sql_write_objects", ex)
        print("db_sql_write_objects", ex)
        pass


async def close_connection(pool):
    pool.close()
    await pool.wait_closed()


async def insert_sign_cards(url, real_id, period, dogovor, dogovor_date, date_publish, type_card, period_start,
                            period_end, comments, done):
    db_pool = None
    try:
        db_pool = await create_connection()
    except aiomysql.OperationalError as ex:
        secure.log.write_log("db_sql_insert_sign_cards", ex)
        await asyncio.sleep(1)
        await insert_sign_cards(url, real_id, period, dogovor, dogovor_date, date_publish, type_card, period_start,
                                period_end, comments, done)
    await write_sign_cards(db_pool, url, real_id, period, dogovor, dogovor_date, date_publish, type_card, period_start,
                           period_end, comments, done)
    await close_connection(db_pool)


async def insert_change_cards(url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url, dogovor_date,
                              date_publish, type_card, period_start, period_end, date_add, comments, done):
    db_pool = None
    try:
        db_pool = await create_connection()
    except aiomysql.OperationalError as ex:
        secure.log.write_log("db_sql_insert_change_cards", ex)
        await asyncio.sleep(1)
        await insert_change_cards(url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url, dogovor_date,
                                  date_publish, type_card, period_start, period_end, date_add, comments, done)
    await write_change_cards(db_pool, url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url,
                             dogovor_date, date_publish, type_card, period_start, period_end, date_add, comments, done)
    await close_connection(db_pool)


async def insert_stop_cards(url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url, reason_stop,
                            dogovor_date, dogovor_stop_date, date_publish, comments, type_card, done):
    db_pool = None
    try:
        db_pool = await create_connection()
    except aiomysql.OperationalError as ex:
        secure.log.write_log("db_sql_insert_stop_cards", ex)
        await asyncio.sleep(1)
        await insert_stop_cards(url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url, reason_stop,
                                dogovor_date, dogovor_stop_date, date_publish, comments, type_card, done)
    await write_stop_cards(db_pool, url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url, reason_stop,
                           dogovor_date, dogovor_stop_date, date_publish, comments, type_card, done)
    await close_connection(db_pool)


async def insert_lessees(url, name, inn, ogrn, table):
    db_pool = None
    try:
        db_pool = await create_connection()
    except aiomysql.OperationalError as ex:
        secure.log.write_log("db_sql_insert_lessees", ex)
        await asyncio.sleep(1)
        await insert_lessees(url, name, inn, ogrn, table)
    await write_lessees(db_pool, url, name, inn, ogrn, table)
    await close_connection(db_pool)


async def insert_lessors(url, name, inn, ogrn, table):
    db_pool = None
    try:
        db_pool = await create_connection()
    except aiomysql.OperationalError as ex:
        secure.log.write_log("db_sql_insert_lessors", ex)
        await asyncio.sleep(1)
        await insert_lessors(url, name, inn, ogrn, table)
    await write_lessors(db_pool, url, name, inn, ogrn, table)
    await close_connection(db_pool)


async def insert_objects(url, object_guid, object_name, object_class, object_description, object_total, table):
    db_pool = None
    try:
        db_pool = await create_connection()
    except aiomysql.OperationalError as ex:
        secure.log.write_log("db_sql_insert_objects", ex)
        await asyncio.sleep(1)
        await insert_objects(url, object_guid, object_name, object_class, object_description, object_total, table)
    await write_objects(db_pool, url, object_guid, object_name, object_class, object_description, object_total, table)
    await close_connection(db_pool)
