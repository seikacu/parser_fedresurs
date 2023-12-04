import asyncio
import datetime
import psutil
import os
import re
import shutil

import secure
import puppet
from secure import log
from selen import multi_pools
from selen import sel_test


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
        destination_path = os.path.join(path_done, file)
        # shutil.move(file_path, destination_path)
        print(f"The files has been moved to {destination_path}")

        # print('-' * 30)

    return all_nums


def start(nums):
    try:
        multi_sel(nums)
    except Exception as ex:
        print("main_start_", ex)
        log.write_log("main_start_", ex)
        pass


def multi_sel(nums):
    threads_num = 12
    data_len = len(nums)
    for el in range(0, data_len, threads_num):
        ids = []
        batch = nums[el:el + threads_num]
        for i in batch:
            ids.append(i)
        threads_num = len(batch)
        print(f'[INFO] {threads_num} process will be launched')
        cpu_count = get_cpu_count()
        multi_pools(cpu_count, ids)


def get_cpu_count():
    return psutil.cpu_count()


def generate_range(start_num, end_num):
    numbers = list(range(start_num, end_num + 1))
    return numbers


def card_search(nums):
    threads_num = 4
    data_len = len(nums)
    for el in range(0, data_len, threads_num):
        ids = []
        batch = nums[el:el + threads_num]
        for i in batch:
            ids.append(i)
        threads_num = len(batch)
        print(f'[INFO] {threads_num} process will be launched')
        cpu_count = get_cpu_count()
        sel_test(ids)


def multi_petter(nums):
    threads_num = 10
    data_len = len(nums)
    for el in range(0, data_len, threads_num):
        ids = []
        batch = nums[el:el + threads_num]
        for i in batch:
            ids.append(i)
        threads_num = len(batch)
        print(f'[INFO] {threads_num} process will be launched')
        asyncio.get_event_loop().run_until_complete(puppet.multi_petts(ids))


def main():
    time_start = datetime.datetime.now()
    star_path = 'data'
    done_path = 'imported'
    result_path = 'result'

    def create_folders():
        if not os.path.exists(star_path):
            os.mkdir(star_path)
        if not os.path.exists(done_path):
            os.mkdir(done_path)
        if not os.path.exists(result_path):
            os.mkdir(result_path)

    create_folders()
    print("start")
    nums = []
    if secure.mode == 2:
        nums = get_nums_list(star_path, done_path)
        multi_sel(nums)
    if secure.mode == 1:
        card_search(nums)
        nums = generate_range(secure.start_num, secure.end_num)
    if secure.mode == 3:
        nums = get_nums_list(star_path, done_path)
        multi_petter(nums)
    print("end")
    time_end = datetime.datetime.now()
    time_diff = time_end - time_start
    tsecs = time_diff.total_seconds()
    print(f"[INFO] Script with {len(nums)} entries worked for {tsecs} seconds.")


'''
    ПРОТЕСТРОВАТЬ CRON
'''
if __name__ == '__main__':
    main()
