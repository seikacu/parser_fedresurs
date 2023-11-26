import pymysql.cursors

from secure import PSql, log


def connect_db():
    connection = pymysql.connect(
        host=PSql.host,
        user=PSql.user,
        password=PSql.password,
        database=PSql.db_name,
        charset=PSql.charset,
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection


def insert_sign_cards(connection, url, real_id, period, dogovor, dogovor_date, date_publish, type_card, period_start,
                      period_end, comments, done):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO ads (url, category, sub_category_1, sub_category_2, sub_category_3, sub_category_4,
                sub_category_5, location, launch_point) VALUES 
                    ('{url}', '{category}', '{sub_category_1}', '{sub_category_2}', '{sub_category_3}',
                    '{sub_category_4}', '{sub_category_5}', '{location}', '{launch_point}');"""
            )

    except Exception as _ex:
        log.write_log("db_sql_insert_to_table ", _ex)
        print("db_sql_insert_to_table_  Error while working with PostgreSQL", _ex)
        pass


def insert_change_cards(connection, url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url,
                        dogovor_date, date_publish, type_card, period_start, period_end, date_add, comments, done):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO ads (url, category, sub_category_1, sub_category_2, sub_category_3, sub_category_4,
                sub_category_5, location, launch_point) VALUES 
                    ('{url}', '{category}', '{sub_category_1}', '{sub_category_2}', '{sub_category_3}',
                    '{sub_category_4}', '{sub_category_5}', '{location}', '{launch_point}');"""
            )

    except Exception as _ex:
        log.write_log("db_sql_insert_to_table ", _ex)
        print("db_sql_insert_to_table_  Error while working with PostgreSQL", _ex)
        pass


def insert_stop_cards(connection, url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url, reason_stop,
                      dogovor_date, dogovor_stop_date, date_publish, comments, type_card, done):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO ads (url, category, sub_category_1, sub_category_2, sub_category_3, sub_category_4,
                sub_category_5, location, launch_point) VALUES 
                    ('{url}', '{category}', '{sub_category_1}', '{sub_category_2}', '{sub_category_3}',
                    '{sub_category_4}', '{sub_category_5}', '{location}', '{launch_point}');"""
            )

    except Exception as _ex:
        log.write_log("db_sql_insert_to_table ", _ex)
        print("db_sql_insert_to_table_  Error while working with PostgreSQL", _ex)
        pass


def add_phone1(connection, id_db, phone):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""UPDATE ads SET phone_1 = '{phone}' WHERE id = {id_db};""")

            print(f"[INFO] Phone_1 {phone} was successfully add, id = {id_db}")
            log.write_log(f"Phone_1 {phone} was successfully add", f"id = {id_db}")
    except Exception as _ex:
        log.write_log("db_sql_add_phone1 ", _ex)
        print("db_sql__add_phone1 Error while working with PostgreSQL", _ex)
        pass


def check_url_in_bd(connection, url):
    with connection.cursor() as cursor:
        cursor.execute(f"""SELECT url FROM ads WHERE url = '{url}';""")
        return cursor.fetchone() is not None


def get_data_from_table(connection, category_name):
    with connection.cursor() as cursor:
        cursor.execute(f"""SELECT id, url FROM ads WHERE launch_point = '{category_name}'
        AND phone_1 IS NULL;""")
        if cursor.fetchone is not None:
            return cursor.fetchall()
