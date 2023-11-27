import pymysql.cursors

from secure import PSql, log


def connect_db():
    connection = pymysql.connect(
        host=PSql.host,
        user=PSql.user,
        password=PSql.password,
        database=PSql.db_name,
        # charset=PSql.charset,
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection


def insert_sign_cards(connection, url, real_id, period, dogovor, dogovor_date, date_publish, type_card, period_start,
                      period_end, comments, done):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO cards_ (url, done, real_id, period, dogovor, dogovor_date, date_publish,
                type, period_start, period_end, comments) VALUES
                ('{url}', '{done}', '{real_id}', '{period}', '{dogovor}', '{dogovor_date}', '{date_publish}',
                '{type_card}', '{period_start}', '{period_end}', '{comments}');"""
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
                f"""INSERT INTO cards_change_ (url, done, real_id, period, dogovor, dogovor_main_real_id,
                dogovor_main_url, dogovor_date, date_publish, type, period_start, period_end, date_add, comments,
                main_card)
                VALUES ('{url}', '{done}', '{real_id}', '{period}', '{dogovor}', '{dogovor_main_real_id}',
                '{dogovor_main_url}', '{dogovor_date}', '{date_publish}', '{type_card}', '{period_start}',
                '{period_end}', '{date_add}', '{comments}', '');"""
            )
    except Exception as _ex:
        log.write_log("db_sql_insert_to_table ", _ex)
        print("db_sql_insert_to_table_  Error while working with PostgreSQL", _ex)
        pass


def insert_stop_cards(connection, url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url,
                      reason_stop, dogovor_date, dogovor_stop_date, date_publish, comments, type_card, done):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO cards_stop_ (url, done, real_id, period, dogovor, dogovor_main_real_id,
                dogovor_main_url, reason_stop, dogovor_date, dogovor_stop_date, date_publish, comments, type) VALUES
                ('{url}', '{done}', '{real_id}', '{period}', '{dogovor}', '{dogovor_main_real_id}',
                '{dogovor_main_url}', '{reason_stop}', '{dogovor_date}', '{dogovor_stop_date}', '{date_publish}',
                '{comments}', '{type_card}');"""
            )
    except Exception as _ex:
        log.write_log("db_sql_insert_to_table ", _ex)
        print("db_sql_insert_to_table_  Error while working with PostgreSQL", _ex)
        pass


def insert_lessees(connection, url, name, inn, ogrn, table):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO {table} (card_id, name, inn, ogrn, nomreg) VALUES 
                    ('{url}', '{name}', '{inn}', '{ogrn}', '');"""
            )
    except Exception as _ex:
        log.write_log("db_sql_insert_to_table ", _ex)
        print("db_sql_insert_to_table_  Error while working with PostgreSQL", _ex)
        pass


def insert_lessors(connection, url, name, inn, ogrn, table):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO {table} (card_id, name, inn, ogrn) VALUES 
                    ('{url}', '{name}', '{inn}', '{ogrn}');"""
            )
    except Exception as _ex:
        log.write_log("db_sql_insert_to_table ", _ex)
        print("db_sql_insert_to_table_  Error while working with PostgreSQL", _ex)
        pass


def insert_objects(connection, url, object_guid, object_name, object_class, object_description, object_total, table):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO {table} (card_id, guid, name, class, description, total, category, category_word_w,
                type, type_word, marka, model) VALUES 
                    ('{url}', '{object_guid}', '{object_name}', '{object_class}', '{object_description}',
                    '{object_total}', '', '', '', '', '', '');"""
            )
    except Exception as _ex:
        log.write_log("db_sql_insert_to_table ", _ex)
        print("db_sql_insert_to_table_  Error while working with PostgreSQL", _ex)
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
