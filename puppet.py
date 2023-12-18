import asyncio
import datetime
import time
import uuid

import secure

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from pyppeteer import errors
from pyppeteer import launch

from db_sql import insert_change_cards
from db_sql import insert_lessees
from db_sql import insert_lessors
from db_sql import insert_objects
from db_sql import insert_sign_cards
from db_sql import insert_stop_cards


def get_period(data):
    period = data.split(' ')[-4] + '-' + data.split(' ')[-2]
    period_start = get_date_format(data.split(' ')[-4], '%d.%m.%Y')
    period_end = get_date_format(data.split(' ')[-2], '%d.%m.%Y')
    return period, period_end, period_start


def get_date_format(date_str, date_format):
    date = datetime.datetime.strptime(date_str, date_format)
    return date


def generate_guid():
    guid = uuid.uuid4()
    return str(guid)


# async def get_bp(data):
#     bp = []
#     inn = 0
#     ogrn = 0
#     description = data.text
#     if description.__contains__('ОГРН'):
#         ogrn = description.split(':')[-1]
#         inn = description.split(':')[-2].split('ОГРН')[0]
#         name = description.split(':')[-3].split('ИНН')[0]
#     elif description.__contains__('ИНН') and description.__contains__('ОГРН') is False:
#         inn = description.split(':')[-1]
#         name = description.split(':')[-2].split('ИНН')[0]
#     else:
#         name = description.split(':')[-1]
#     if name.__contains__('Лизингодатели'):
#         name = name.split('Лизингодатели')[1]
#     elif name.__contains__('Лизингополучатели'):
#         name = name.split('Лизингополучатели')[1]
#     name = name.replace("'", '"')
#     name = name.replace('\n', '')
#     bp.append(name)
#     bp.append(inn)
#     bp.append(ogrn)
#     return bp


def change_proxy():
    num_procs = secure.num_proxs
    if secure.PROXY_ID < num_procs - 1:
        secure.PROXY_ID += 1
    else:
        secure.PROXY_ID = 0
    # print('[INFO] CHANGE PROXY')
    # secure.log.write_log("CHANGE PROXY: ", f'new PROXY_ID: {secure.PROXY_ID}')


async def parse(page, json_dict):
    page_data = json_dict['pageData'][0]
    url = page_data['guid']
    link = f'https://fedresurs.ru/sfactmessage/{url}'

    '''
        Лизингополучатель
    '''
    lessee = page_data['weakSide'][0]
    lessee_name = lessee['name']
    lessee_inn = 0
    if 'inn' in lessee:
        lessee_inn = lessee['inn']
    lessee_ogrn = 0
    if 'ogrn' in lessee:
        lessee_ogrn = lessee['ogrn']

    '''
        Лизингодатель
    '''
    lessor = page_data['strongSide'][0]
    lessor_name = lessor['name']
    lessor_inn = 0
    if 'inn' in lessor:
        lessor_inn = lessor['inn']
    lessor_ogrn = 0
    if 'ogrn' in lessor:
        lessor_ogrn = lessor['ogrn']

    done = 0
    '''
        CARDS
    '''
    # url = link.split('/')[-1].replace('-', '').upper()
    type_card = page_data['type']
    real_id = page_data['number']
    period = ''
    dogovor = ''
    dogovor_date = None
    date_publish = str(page_data['publishDate']).split('T')[0]
    date_publish = get_date_format(date_publish, '%Y-%m-%d')
    period_start = None
    period_end = None
    comments = ''
    """
        CARDS_CHANGE
    """
    dogovor_main_real_id = ''
    dogovor_main_url = ''
    date_add = None
    """
        CARDS_STOP
    """
    reason_stop = ''
    dogovor_stop_date = None
    # '''
    #     CARD_LESSORS
    #     Лизингодатели
    # '''
    # lessor_name = ''
    # lessor_inn = 0
    # lessor_ogrn = 0
    # '''
    #     CARD_LESSEES
    #     Лизингополучатели
    # '''
    # lessees_name = ''
    # lessees_inn = 0
    # lessees_ogrn = 0
    '''
        CARD_OBJECTS
    '''
    object_name = ''
    object_class = ''
    object_description = ''
    object_total = ''

    status = ''

    try:
        await page.goto(link, {'waitUntil': 'domcontentloaded'})
        await page.waitForSelector('div[class="subject"]')  # timeout=2000
        res = await page.content()
        soup = BeautifulSoup(res, "lxml")
        '''
        WORK WITH CARD
        '''
        subject = soup.find('div', class_='subject')
        if subject:
            operation = subject.find('h2').text.split(' ')[0].lower()
            if operation == 'заключение':
                status = 'sign'
                # type_card = 'FinancialLeaseContract'
            elif operation == 'изменение':
                status = 'change'
                # type_card = 'ChangeFinancialLeaseContract'
            elif operation == 'прекращение':
                status = 'stop'
                # type_card = 'StopFinancialLeaseContract'
        # spans = subject.find_all('span')
        # if spans:
        #     real_id = spans[-2].text.split('№')[1]
        #     date_publish = get_date_format(spans[-1].text.split(' ')[2], '%d.%m.%Y')
        cards = soup.find_all('div', class_='card-section')
        if cards:
            '''
            TABLE CARDS
            '''
            dogovor = cards[1].find_next('span', text=lambda text: text and 'Договор' in text)
            if dogovor:
                dog = dogovor.find_parent('div').find_parent('div')
                if dog:
                    dogovor_text = dog.text
                    dogovor = dogovor_text.split(' от ')[0]
                    if dogovor_text.__contains__(':'):
                        dogovor = dogovor.split(':')[1]
                    else:
                        dogovor = dogovor.split('Договор')[1]
                    dogovor_date = get_date_format(dogovor_text.split(' от ')[1], '%d.%m.%Y')
            srok = cards[1].find_next('div', text=lambda text: text and 'Срок финансовой аренды' in text)
            if srok:
                srok_text = srok.find_next('div').text
                period, period_end, period_start = get_period(srok_text)
            if status == 'stop' or status == 'change':
                main_dog = cards[1].find_next('div', text=lambda text: text and 'Сообщение' in text)
                if main_dog:
                    tag_a = main_dog.find_parent('div').find_next('a')
                    if tag_a:
                        dogovor_main_url = tag_a.get('href').split('/')[-1]
                        dogovor_main_real_id = tag_a.text.split(' от ')[0][1:]
            if status == 'change':
                date_add = time.strftime('%Y-%m-%d %H:%M:%S')
            if status == 'stop':
                date_stop = (cards[1].find_next('div', text=lambda text: text and 'Дата прекращения' in text)
                             .find_next('div').text)
                dogovor_stop_date = get_date_format(date_stop, '%d.%m.%Y')
                reason_stop = (cards[1].find_next('div', text=lambda text: text and 'Причина прекращения' in text)
                               .find_next('div').text)
            comm = cards[1].find('div', 'div',
                                 text=lambda text: text and 'Комментарий пользователя' in text)
            if comm:
                comments = comm.find_parent('div').find_next('span').text
            # '''
            # TABLE CARD_LESSORS
            # '''
            # field_lessors = None
            # lessors = cards[1].find_next('div', text=lambda text: text and 'Лизингодатели' in text)
            # if lessors:
            #     field_lessors = lessors.find_parent('div')
            # else:
            #     secure.log.write_log(f'There is no lessor by url - {link}', '')
            # if field_lessors:
            #     bp = await get_bp(field_lessors)
            #     lessor_name = bp[0]
            #     lessor_inn = bp[1]
            #     lessor_ogrn = bp[2]
            # '''
            # TABLE CARD_LESSEES
            # '''
            # field_lessees = None
            # lessees = cards[1].find_next('div', text=lambda text: text and 'Лизингополучатели' in text)
            # if lessees:
            #     field_lessees = lessees.find_parent('div')
            # else:
            #     secure.log.write_log(f'There is no lessee by url - {link}', '')
            # if field_lessees:
            #     bp = await get_bp(field_lessees)
            #     lessees_name = bp[0]
            #     lessees_inn = bp[1]
            #     lessees_ogrn = bp[2]
            '''
            TABLE CARD_OBJECTS
            '''
            table = cards[1].find_next('table')
            if table:
                object_guid = generate_guid()
                heading = table.find_all_next('th')
                tbody = table.find_next('tbody')
                if heading[0].text == 'Идентификатор':
                    td_tags = table.find_all_next('td')
                    td_tags_len = len(td_tags)
                    step = 3
                    for row in range(0, td_tags_len, step):
                        batch = td_tags[row:row + step]
                        object_name = batch[0].text
                        object_class = batch[1].find_next('span').text
                        object_description = batch[2].text.rstrip()
                        object_total = f'{object_name} {object_class} {object_description}'
                if status == 'sign':
                    await insert_objects(url, object_guid, object_name, object_class, object_description,
                                         object_total, 'card_objects')
                elif status == 'change':
                    await insert_objects(url, object_guid, object_name, object_class, object_description,
                                         object_total, 'card_objects_change')
                    '''
                        ЕСЛИ БУДЕТ ТАБЛИЦА card_objects_stop, ТОГДА МОЖНО РАСКОММЕНТИРОВАТЬ
                        запись данных в объекты лизинга по прекращенным договорам
                    '''
                # elif status == 'stop':
                #     await insert_objects(url, object_guid, object_name, object_class, object_description,
                #                          object_total, 'card_objects_stop')
                else:
                    td_tags = tbody.find_all_next('td')
                    td_tags_len = len(td_tags)
                    step = 3
                    for row in range(0, td_tags_len, step):
                        batch = td_tags[row:row + step]
                        object_name = (batch[2].find_next('div', string=' Идентификатор ').find_next('div')
                                       .text.strip())
                        object_class = batch[1].find_next('span').text
                        object_description = (batch[2].find_next('div', string=' Описание ').find_next('div').text
                                              .strip())
                        object_total = f'{object_name} {object_class} {object_description}'
                        if status == 'sign':
                            await insert_objects(url, object_guid, object_name, object_class, object_description,
                                                 object_total, 'card_objects')
                        elif status == 'change':
                            await insert_objects(url, object_guid, object_name, object_class, object_description,
                                                 object_total, 'card_objects_change')
                        '''
                            ЕСЛИ БУДЕТ ТАБЛИЦА card_objects_stop, ТОГДА МОЖНО РАСКОММЕНТИРОВАТЬ
                            запись данных в объекты лизинга по прекращенным договорам
                        '''
                        # elif status == 'stop':
                        #     await insert_objects(url, object_guid, object_name, object_class, object_description,
                        #                          object_total, 'card_objects_stop')
        if status == 'sign':
            await insert_sign_cards(url, real_id, period, dogovor, dogovor_date, date_publish, type_card,
                                    period_start, period_end, comments, done)
            await insert_lessees(url, lessee_name, lessee_inn, lessee_ogrn, 'card_lessees')
            await insert_lessors(url, lessor_name, lessor_inn, lessor_ogrn, 'card_lessors')
        elif status == 'change':
            await insert_change_cards(url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url,
                                      dogovor_date, date_publish, type_card, period_start, period_end, date_add,
                                      comments, done)
            await insert_lessees(url, lessee_name, lessee_inn, lessee_ogrn, 'card_lessees_change')
            await insert_lessors(url, lessor_name, lessor_inn, lessor_ogrn, 'card_lessors_change')
        elif status == 'stop':
            await insert_stop_cards(url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url,
                                    reason_stop, dogovor_date, dogovor_stop_date, date_publish, comments,
                                    type_card, done)
            await insert_lessees(url, lessee_name, lessee_inn, lessee_ogrn, 'card_lessees_stop')
            await insert_lessors(url, lessor_name, lessor_inn, lessor_ogrn, 'card_lessors_stop')
    except errors.TimeoutError as ex:
        await fetch_catch_error(ex, real_id, page)
        await parse(page, json_dict)
        pass
    except errors.ElementHandleError as ex:
        await fetch_catch_error(ex, real_id, page)
        await parse(page, json_dict)
        pass


async def fetch(id_db, json_dict):
    time_start = datetime.datetime.now()
    browser = None

    try:
        ua = UserAgent().chrome
        # ua = ('Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) '
        #       'Version/16.6 Mobile/15E148 Safari/604.1')
        # browser_path = '/bin/chromium-browser'
        browser_path = '/usr/bin/chromium'
        change_proxy()
        proxy_ip, proxy_port, proxy_user, proxy_pass = secure.get_proxy_pref(3)
        # width, height = 1366, 768
        width, height = 1000, 1000
        # width, height = 375, 667
        # width, height = 800, 600
        start_parm = ({
            'executablePath': f'{browser_path}',
            'headless': True,
            # 'ignoreHTTPSErrors': True,
            # 'dumpio': True,
            'autoClose': False,
            'args': [
                f'--window-size={width},{height}',
                '--disable-infobars',
                '--disable-dev-shm-usage',
                # '--unlimited-storage',
                # '--full-memory-crash-report',
                '--log-level=30',
                f'--user-agent={ua}',
                '--no-sandbox',
                f'--proxy-server={proxy_ip}:{proxy_port}'
                # '--start-maximized'
            ]
        })
        browser = await launch(**start_parm)
        page = await browser.newPage()
        await page.setViewport({'width': width, 'height': height, 'deviceScaleFactor': 1})
        await page.authenticate({'username': proxy_user, 'password': proxy_pass})
        js_text = """
            () =>{ 
                Object.defineProperties(navigator,{ webdriver:{ get: () => false } });
                window.navigator.chrome = { runtime: {},  };
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5,6], });
             }
                """
        await page.evaluateOnNewDocument(js_text)
        '''
            отключение загрузки картинок
        '''
        await page.setRequestInterception(True)

        async def intercept(request):
            if any(request.resourceType == _ for _ in ('stylesheet', 'image', 'font')):
                await request.abort()
            else:
                await request.continue_()

        page.on('request', lambda req: asyncio.ensure_future(intercept(req)))

        await parse(page, json_dict)

        time_end = datetime.datetime.now()
        time_diff = time_end - time_start
        tsecs = time_diff.total_seconds()
        print(f"[INFO] Script scrap page {tsecs} seconds.")
    except errors.PageError as ex:
        await fetch_catch_error(ex, id_db, None)
        await fetch(id_db, json_dict)
        pass
    except errors.BrowserError as ex:
        await fetch_catch_error(ex, id_db, None)
        await fetch(id_db, json_dict)
        pass
    except errors.NetworkError as ne:
        await fetch_catch_error(ne, id_db, None)
        await fetch(id_db, json_dict)
        pass
    finally:
        if browser:
            await browser.close()


async def fetch_catch_error(ex, id_db, page):
    print(f'Repeat card parsing (func fetch(id_db)) by id card - {id_db}, because {ex}')
    secure.log.write_log(f'Repeat card parsing (func fetch(id_db)) by id card - {id_db} _ ', ex)
    if page:
        res = await page.content()
        await page.screenshot({'path': f'result/page_error_id_{id_db}.png', 'fullPage': True})
        with open(f"result/{id_db}.html", "w", encoding="utf-8") as file:
            file.write(res)


async def multi_petts(ids):
    await asyncio.gather(*[fetch(el) for el in ids])
