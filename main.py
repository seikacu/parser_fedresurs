import os
import re
import shutil

from db_sql import connect_db
from secure import log
from selen import multi_selen


def get_num_line(line):
    numbers = re.findall(r'\d+', line)
    return [int(num) for num in numbers]


def get_nums_list(start_path, path_done):
    all_nums = []

    files = [f for f in os.listdir(start_path) if f.endswith('.txt')]
    for file in files:
        file_path = os.path.join(start_path, file)
        # print(f'Чтение файла: {file}')
        with open(file_path) as f:
            for line_number, line in enumerate(f, 1):
                # print(f'Строка {line_number}: {line.strip()}')
                num = get_num_line(line)
                all_nums.extend(num)
                # size = len(line.strip())
                # print(f'line len is {size}')

        # Перемещение прочитанного файла в отдельную папку
        # destination_path = os.path.join(path_done, file)
        # shutil.move(file_path, destination_path)
        # print(f"Файл перемещен в {destination_path}")

        # print('-' * 30)

    return all_nums


def start(nums):
    connection = None
    try:
        connection = connect_db()
        connection.autocommit = True

        threads_num = 10
        data_len = len(nums)
        ids = []
        for el in range(0, data_len, threads_num):
            batch = nums[el:el + threads_num]
            for i in batch:
                ids.append(i)
            threads_num = len(batch)
        print(f'Будет запущено {threads_num} параллельных потоков')
        multi_selen(connection, threads_num, ids)

    except Exception as _ex:
        print("main_start_", _ex)
        log.write_log("main_start_", _ex)
        pass
    finally:
        if connection:
            connection.close()
            print("[INFO] Сбор данных закончен")


def main():
    star_path = 'data'
    done_path = 'imported'

    def create_folders():
        if not os.path.exists(star_path):
            os.mkdir(star_path)
        if not os.path.exists(done_path):
            os.mkdir(done_path)

    create_folders()
    nums = get_nums_list(star_path, done_path)
    print("start")
    start(nums)

    # print(nums)
    print("end")


if __name__ == '__main__':
    main()
