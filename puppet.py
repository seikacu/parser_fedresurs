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


async def parse(page, link):
    done = 0

    '''
    CARDS
    '''
    type_card = ''
    url = ''
    real_id = ''
    period = ''
    dogovor = ''
    dogovor_date = None
    date_publish = None
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

    '''
    CARD_LESSORS
    Лизингодатели
    '''
    lessor_name = ''
    lessor_inn = 0
    lessor_ogrn = 0

    '''
    CARD_LESSEES
    Лизингополучатели
    '''
    lessees_name = ''
    lessees_inn = 0
    lessees_ogrn = 0

    '''
    CARD_OBJECTS
    '''
    object_name = ''
    object_class = ''
    object_description = ''
    object_total = ''
    object_guid = ''

    status = ''
    try:
        await page.goto(link)
        # await page.content()
        # await page.waitForSelector('body')

        # wait for search results to load
        # await page.waitFor(1000)
        # await page.screenshot({'path': 'result/screenshot3.png'})
        await page.waitForSelector('div[class="subject"]', timeout=1000)
        res = await page.content()
        # print(page_text)
        # secure.log.write_log('page_content', page_text)

        # with open(f"result/tst.html", "w", encoding="utf-8") as file:
        #     file.write(res)
        soup = BeautifulSoup(res, "lxml")
        '''
        WORK WITH CARD
        '''
        subject = soup.find('div', class_='subject')
        if subject:
            operation = subject.find('h2').text.split(' ')[0].lower()
            if operation == 'заключение':
                status = 'sign'
                type_card = 'FinancialLeaseContract'
            elif operation == 'изменение':
                status = 'change'
                type_card = 'ChangeFinancialLeaseContract'
            elif operation == 'прекращение':
                status = 'stop'
                type_card = 'StopFinancialLeaseContract'

        url = link.split('/')[-1].replace('-', '').upper()

        spans = subject.find_all('span')
        if spans:
            real_id = spans[-2].text.split('№')[1]
            date_publish = get_date_format(spans[-1].text.split(' ')[2], '%d.%m.%Y')

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

            '''
            TABLE CARD_LESSORS
            '''
            field_lessors = (cards[1].find_next('div', text=lambda text: text and 'Лизингодатели' in text)
                             .find_parent('div'))
            if field_lessors:
                bp = await get_bp(field_lessors)
                lessor_name = bp[0]
                lessor_inn = bp[1]
                lessor_ogrn = bp[2]

            '''
            TABLE CARD_LESSEES
            '''
            field_lessees = (cards[1].find_next('div', text=lambda text: text and 'Лизингополучатели' in text)
                             .find_parent('div'))
            if field_lessees:
                bp = await get_bp(field_lessees)
                lessees_name = bp[0]
                lessees_inn = bp[1]
                lessees_ogrn = bp[2]

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
                        # if status == 'sign' or status == 'change':
                        #     await insert_objects(url, object_guid, object_name, object_class, object_description,
                        #                          object_total, 'card_objects_')
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
                        # if status == 'sign' or status == 'change':
                        #     await insert_objects(url, object_guid, object_name, object_class, object_description,
                        #                          object_total, 'card_objects_')

            # if status == 'sign':
            #     await insert_sign_cards(url, real_id, period, dogovor, dogovor_date, date_publish, type_card,
            #                             period_start, period_end, comments, done)
            #     await insert_lessees(url, lessees_name, lessees_inn, lessees_ogrn, 'card_lessees_')
            #     await insert_lessors(url, lessor_name, lessor_inn, lessor_ogrn, 'card_lessors_')
            # elif status == 'change':
            #     await insert_change_cards(url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url,
            #                               dogovor_date, date_publish, type_card, period_start, period_end, date_add,
            #                               comments, done)
            #     await insert_lessees(url, lessees_name, lessees_inn, lessees_ogrn, 'card_lessees_change_')
            #     await insert_lessors(url, lessor_name, lessor_inn, lessor_ogrn, 'card_lessors_change_')
            # elif status == 'stop':
            #     await insert_stop_cards(url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url,
            #                             reason_stop, dogovor_date, dogovor_stop_date, date_publish, comments,
            #                             type_card, done)
            #     await insert_lessees(url, lessees_name, lessees_inn, lessees_ogrn, 'card_lessees_stop_')
            #     await insert_lessors(url, lessor_name, lessor_inn, lessor_ogrn, 'card_lessors_stop_')
    except errors.TimeoutError as ex:
        print(ex)
        await parse(page, link)
        pass
        '''
            В ПРЕКРАЩЕНИИ ДОГОВОРА ПРИСУТСТВУЕТ ТАБЛИЦА OBJECTS
        '''


def get_period(data):
    period = data.split(' ')[-4] + '-' + data.split(' ')[-2]
    period_start = get_date_format(data.split(' ')[-4], '%d.%m.%Y')
    period_end = get_date_format(data.split(' ')[-2], '%d.%m.%Y')
    return period, period_end, period_start


async def fetch(id_db):
    # width, height = 1366, 768
    width, height = 500, 500

    browser = None

    done = 0

    '''
    CARDS
    '''
    type_card = ''
    url = ''
    real_id = id_db
    period = ''
    dogovor = ''
    dogovor_date = None
    date_publish = None
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

    '''
    CARD_LESSORS
    Лизингодатели
    '''
    lessor_name = ''
    lessor_inn = 0
    lessor_ogrn = 0

    '''
    CARD_LESSEES
    Лизингополучатели
    '''
    lessees_name = ''
    lessees_inn = 0
    lessees_ogrn = 0

    '''
    CARD_OBJECTS
    '''
    object_name = ''
    object_class = ''
    object_description = ''
    object_total = ''
    object_guid = ''

    status = ''

    try:
        time_start = datetime.datetime.now()
        # Сначала удалите параметры автоматизации браузера
        # Удалить параметры запуска автоматизации
        # launcher.AUTOMATION_ARGS.remove("--enable-automation")
        ua = UserAgent().chrome
        # ua = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
        #       'sChrome/73.0.3683.103 Safari/537.36')
        # browser_path = '/bin/chromium-browser'
        proxy_ip, proxy_port, proxy_user, proxy_pass = secure.get_proxy_pref(3)
        browser_path = '/usr/bin/chromium'
        start_parm = ({
            'executablePath': f'{browser_path}',
            'headless': True,
            # 'ignoreHTTPSErrors': True,
            # 'dumpio': True,
            'autoClose': True,
            'args': [
                f'--window-size={width},{height}',
                '--disable-infobars',
                # Уровень сохранения журнала. Рекомендуется установить лучший, иначе созданный журнал будет занимать
                # многоместа. 30 - уровень предупреждения
                '--log-level=30',
                f'--user-agent={ua}',
                '--no-sandbox',  # Отключить режим песочницы
                f'--proxy-server={proxy_ip}:{proxy_port}',
                # '--start-maximized'
            ]
        })

        browser = await launch(**start_parm)
        page = await browser.newPage()
        await page.setViewport({'width': width, 'height': height})
        await page.authenticate({'username': proxy_user, 'password': proxy_pass})

        js_text = """
        () =>{ 
            Object.defineProperties(navigator,{ webdriver:{ get: () => false } });
            window.navigator.chrome = { runtime: {},  };
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5,6], });
         }
            """
        # Значение остается неизменным после обновления этой страницы, и js выполняется автоматически
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
        link = 'https://fedresurs.ru/search/encumbrances'
        await page.goto(link)
        # await page.content()
        # await page.waitForSelector('body')
        title = await page.title()
        if title and '429' in title:
            print('!!!CHANGE PROXY!!!')
            await fetch(id_db)
        # await page.waitFor(1000)

        # wait for search results to load
        # await page.waitFor(3000)
        # await page.screenshot({'path': 'result/screenshot.png'})

        '''
        SEARCH
        '''
        # but_close = await page.waitForSelector('span.btn-close')
        # if but_close:
        #     await but_close.click()
        # await page.screenshot({'path': 'result/screenshot1.png'})
        open_expand_form = await page.waitForSelector('a.open_expand_form', timeout=2000)
        if open_expand_form:
            await open_expand_form.click()
        input_search = await page.waitForSelector('input[type="text"]')
        if input_search:
            await input_search.type(str(id_db))
        val = await page.waitForSelector('div[class*="value"]')
        if val:
            await val.click()
            options = await page.waitForSelector('div[class*="options"]')
            if options:
                lis = await options.querySelectorAll('li')
                for li in lis:
                    name = await page.evaluate('(element) => element.textContent', li)
                    name = str(name).lower().strip()
                    if name == 'лизинг':
                        await li.click()
                        break
        # checkbox = await page.querySelector('input#additionalSearchFnp')
        # if checkbox:
        #     await checkbox.click()
        but_submit = await page.waitForSelector('button[type="submit"]')
        if but_submit:
            await but_submit.click()
        '''
        GET CARD
        '''
        '''
            ПЕРЕДЕЛАТЬ ДЛЯ ПОИСКА ПО НОМЕРАМ РЕАЛЬНЫХ КАРТОЧЕК И СГЕНЕРИРОВАННЫХ
        '''
        load_info = None
        try:
            load_info = await page.waitForSelector('.load-info .no-result-msg', timeout=500)
        except errors.TimeoutError:
            pass
        if load_info:
            print(f'The card with the number {id_db} was not found')
        else:
            '''
                ДОБАВИТЬ ПРОВЕРКУ НА ЧТО КАРТОЧКА СУЩЕСТВУЕТ
            '''
            card_link = await page.waitForSelector('div.encumbrances-result__body', timeout=2000)
            # card_link = await page.waitForXPath('//div[contains(@class, "encumbrances-result__body")]')
            if card_link:
                a = await card_link.querySelector('a')
                href = await page.evaluate('(element) => element.getAttribute("href")', a)
                link = f'https://fedresurs.ru{href}'
                await page.goto(link)
                # await page.content()
                # await page.waitForSelector('body')

                # wait for search results to load
                # await page.waitFor(1000)
                # await page.screenshot({'path': 'result/screenshot3.png'})
                await page.waitForSelector('div[class="subject"]', timeout=1000)
                res = await page.content()

                await parse(page, link)
            else:
                print('No card link!')
        time_end = datetime.datetime.now()
        time_diff = time_end - time_start
        tsecs = time_diff.total_seconds()
        print(f"[INFO] Script scrap page {tsecs} seconds.")
    except errors.TimeoutError as ex:
        print(ex)
        if secure.mode != 1:
            await fetch(id_db)
        pass
    finally:
        if browser:
            await browser.close()


def petter_start(ids):
    asyncio.get_event_loop().run_until_complete(multi_petts(ids))


async def multi_petts(ids):
    await asyncio.gather(*[fetch(el) for el in ids])


def get_date_format(date_str, date_format):
    date = datetime.datetime.strptime(date_str, date_format)
    return date


def generate_guid():
    guid = uuid.uuid4()
    return str(guid)


async def get_bp(data):
    bp = []
    inn = 0
    ogrn = 0
    description = data.text
    if description.__contains__('ОГРН'):
        ogrn = description.split(':')[-1]
        inn = description.split(':')[-2].split('ОГРН')[0]
        name = description.split(':')[-3].split('ИНН')[0]
    elif description.__contains__('ИНН') and description.__contains__('ОГРН') is False:
        inn = description.split(':')[-1]
        name = description.split(':')[-2].split('ИНН')[0]
    else:
        name = description.split(':')[-1]
    if name.__contains__('Лизингодатели'):
        name = name.split('Лизингодатели')[1]
    elif name.__contains__('Лизингополучатели'):
        name = name.split('Лизингополучатели')[1]
    bp.append(name.replace("'", '"'))
    bp.append(inn)
    bp.append(ogrn)
    return bp


def change_proxy():
    num_procs = secure.num_proxs
    if secure.PROXY_ID < num_procs - 1:
        secure.PROXY_ID += 1
    else:
        secure.PROXY_ID = 0
    print('[INFO] CHANGE PROXY')
    secure.log.write_log("CHANGE PROXY: ", f'new PROXY_ID: {secure.PROXY_ID}')
