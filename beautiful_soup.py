import asyncio
import json
import os
import re

# import aiohttp
import requests

from bs4 import BeautifulSoup
from tqdm import tqdm

from secure import log, proxy
from db_sql import check_url_in_bd, insert_to_table


def get_category():
    """
    получить категорию
    """
    with open("data/all_categories_dict.json", encoding="utf-8") as file:
        all_categories = json.load(file)
    category = list()
    for key, value in all_categories.items():
        category_name = value[0]
        category.append(f"{key} - {category_name}")
    return category


def get_data(link, launch_point, selection, progress_callback, connection, prev_link=None):
    launch_point_split = launch_point.split("_")
    url = f"https://www.ss.lv{link}"
    link_split = link.split("/")
    # исключаем ссылки из других категорий
    if link_split[2] != launch_point_split[0]:
        log.write_log("Исключаем ссылку из другой категории - ", link)
        return

    if len(link_split) > 4:
        if prev_link not in link:
            log.write_log("Исключаем перекрестную ссылку в категории - ", link)
            return
        elif "exchange" in link:
            log.write_log("exchange - ", link)
            return

    soup = get_soup(2, url, link_split)
    pages = soup.find("a", class_="navi")

    if pages is not None:
        scrap_data(soup, launch_point, selection, url, connection)  # , progress_callback
    elif check_sub_category(soup):
        sub_cats = soup.find_all("h4", class_="category")
        total_subcategories = len(sub_cats)
        for idx, item in tqdm(enumerate(sub_cats), total=total_subcategories,
                              desc="Processing subcategories", unit=f"subcategory - {link}"):
            item_href = item.find("a").get("href")
            get_data(item_href, launch_point, selection, progress_callback, connection, link)
            progress_callback((idx + 1) / total_subcategories)


def href(selection: str):
    with open("data/all_categories_dict.json", encoding="utf-8") as file:
        all_categories = json.load(file)
    for key, value in all_categories.items():
        category_name = value[0]
        category_link = value[1]
        val = f"{key} - {category_name}"
        if val == selection:
            return category_link


def get_soup(mode, url, url_split):
    headers = {
        "Referer": "https://www.ss.lv/",
        "Sec-Ch-Ua": '"Chromium";v="116", "Not)A;Brand";v="24", "YaBrowser";v="23"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "Windows",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/116.0.5845.967 YaBrowser/23.9.1.967 Yowser/2.5 Safari/537.36"
    }
    file_name = ""

    if mode == 2:
        if len(url_split) == 4:
            file_name = url_split[2]
        if len(url_split) == 5:
            file_name = url_split[3]
        if len(url_split) == 6:
            file_name = url_split[4]
        if len(url_split) == 7:
            file_name = url_split[5]
        if len(url_split) == 8:
            file_name = url_split[6]
        if len(url_split) == 9:
            file_name = url_split[7]
        if len(url_split) == 10:
            file_name = url_split[8]

    if not file_name.endswith(".html"):
        file_name += ".html"
    r = requests.get(url=url, headers=headers, allow_redirects=False, proxies=proxy)
    if not os.path.exists("data"):
        os.mkdir("data")
    with open(f"data/{file_name}", "w", encoding="utf-8") as file:
        file.write(r.text)
    with open(f"data/{file_name}", encoding="utf-8") as file:
        src = file.read()
    soup = BeautifulSoup(src, "lxml")
    return soup


def check_sub_category(soup: BeautifulSoup):
    sub_cat = soup.find_all("h4", class_="category")
    if len(sub_cat) > 0:
        return True
    else:
        return False


def scrap_data(soup: BeautifulSoup, launch_point, selection, url, connection):  # , progress_callback
    pages = soup.find("a", class_="navi")
    if pages is not None:
        pages_count = pages.get("href")
        digit = re.findall(r'\d+', pages_count)
        count = int(digit[0])
        for i in range(1, count + 1):
            lin = f"{url}page{i}.html"
            lin_split = lin.split("/")
            soup = get_soup(2, lin, lin_split)
            asyncio.run(gather_data(launch_point, selection, soup, connection))
    else:
        asyncio.run(gather_data(launch_point, selection, soup, connection))


async def get_page_data(session, link, launch_point, selection, connection):
    headers = {
        "Referer": "https://www.ss.lv/",
        "Sec-Ch-Ua": '"Chromium";v="116", "Not)A;Brand";v="24", "YaBrowser";v="23"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "Windows",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
                      "Chrome/116.0.5845.967 YaBrowser/23.9.1.967 Yowser/2.5 Safari/537.36"
    }
    url = f"https://www.ss.lv{link}"
    try:
        async with (session.get(url=url, headers=headers, allow_redirects=False) as response):
            response_text = await response.text()
            url_split = link.split("/")
            file_name = url_split[len(url_split) - 1]
            with open(f"data/{file_name}", "w", encoding="utf-8") as file:
                file.write(response_text)
            with open(f"data/{file_name}", encoding="utf-8") as file:
                src = file.read()
            soup = BeautifulSoup(src, "lxml")
            sel_split = selection.split("-")
            category = sel_split[1]
            links = soup.select('h2.headtitle a')
            location = ""
            sub_category_1 = ""
            sub_category_2 = ""
            sub_category_3 = ""
            sub_category_4 = ""
            sub_category_5 = ""
            for i in range(0, len(links)):
                if i == 0:
                    sub_category_1 = links[0].text
                if i == 1:
                    sub_category_2 = links[1].text
                if i == 2:
                    sub_category_3 = links[2].text
                if i == 3:
                    sub_category_4 = links[3].text
                if i == 4:
                    sub_category_5 = links[4].text

            find_location = soup.find('td', {'class': 'ads_contacts_name'}, text='Место:')
            if find_location is not None:
                location = find_location.find_next_sibling('td').text

            await insert_to_table(connection, url, category, sub_category_1, sub_category_2, sub_category_3,
                                  sub_category_4, sub_category_5, location, launch_point)
    except AttributeError as AE:
        log.write_log("get_page_data ", AE)
        print(AE)


async def gather_data(launch_point, selection, soup, connection):
    # async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(), trust_env=True) as session:
        tasks = []
        div_d1_tags = soup.find_all('div', {'class': 'd1'})
        for div_tag in div_d1_tags:
            links = div_tag.find_all('a')
            for link in links:
                url = f"https://www.ss.lv{link['href']}"
                if check_url_in_bd(connection, url):
                    continue
                # task = asyncio.create_task(get_page_data(session, link['href'], launch_point, selection, connection))
                # tasks.append(task)
            await asyncio.gather(*tasks)
