import asyncio
import datetime
import os
import re
import shutil

import secure

from parse import find_cards


def get_num_line(line):
    numbers = re.findall(r'\d+', line)
    return [int(num) for num in numbers]


def get_nums_list(start_path, path_done):
    all_nums = []
    files = [f for f in os.listdir(start_path) if f.endswith('.txt')]
    for file in files:
        file_path = os.path.join(start_path, file)
        with open(file_path) as f:
            for line in enumerate(f, 1):
                num = get_num_line(line)
                all_nums.extend(num)
        destination_path = os.path.join(path_done, file)
        shutil.move(file_path, destination_path)
        print(f"The files has been moved to {destination_path}")
    return all_nums


def generate_range(start_num, end_num):
    numbers = list(range(start_num, end_num + 1))
    return numbers


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
    if secure.mode == 1:
        nums = generate_range(secure.start_num, secure.end_num)
        asyncio.get_event_loop().run_until_complete(find_cards(nums))
    if secure.mode == 2:
        nums = get_nums_list(star_path, done_path)
        asyncio.get_event_loop().run_until_complete(find_cards(nums))
    print("end")
    time_end = datetime.datetime.now()
    time_diff = time_end - time_start
    tsecs = time_diff.total_seconds()
    print(f"[INFO] Script with {len(nums)} entries worked for {tsecs} seconds.")


if __name__ == '__main__':
    main()
