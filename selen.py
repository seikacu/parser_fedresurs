import threading
import time
import zipfile

import secure

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as ex_cond
from fake_useragent import UserAgent
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from db_sql import insert_sign_cards, insert_change_cards, insert_stop_cards


def set_driver_options(options):
    # безголовый режим браузера
    # options.add_argument('--headless=new')
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-notifications")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument("--disable-blink-features=AutomationControlled")
    prefs = {
        'profile.managed_default_content_settings.images': 2,
    }
    options.add_experimental_option("prefs", prefs)


def get_selenium_driver(use_proxy, num_proxy):
    ua = UserAgent()
    options = webdriver.ChromeOptions()
    set_driver_options(options)

    if use_proxy:
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        plugin_file = 'proxy_auth_plugin.zip'

        with zipfile.ZipFile(plugin_file, 'w') as zp:
            zp.writestr('manifest.json', secure.get_proxy_pref(num_proxy, 0))
            zp.writestr('background.js', secure.get_proxy_pref(num_proxy, 1))

        options.add_extension(plugin_file)

    options.add_argument(f'--user-agent={ua.random}')

    caps = DesiredCapabilities().CHROME
    # caps['pageLoadStrategy'] = 'normal'
    caps['pageLoadStrategy'] = 'eager'

    service = Service(ChromeDriverManager().install(), desired_capabilities=caps)
    driver = webdriver.Chrome(service=service, options=options)

    return driver


def get_element_text(driver: webdriver.Chrome, path: str) -> str:
    try:
        return driver.find_element(By.XPATH, path).text
    except NoSuchElementException:
        return ''


def fill_data(connection, id_db):
    driver = None
    # link = f'https://fedresurs.ru/search/encumbrances?offset=0&limit=15&searchString={id_db}&group=Leasing'
    link = 'https://fedresurs.ru/search/encumbrances'

    done = 0

    '''
    ПУБЛИКАТОР - куда его грузить???
    '''
    publisher_name = ''
    publisher_inn = 0
    publisher_ogrn = 0

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

    status = ''

    try:
        driver = get_selenium_driver(True, secure.GLOB_ID)
        driver.get(link)

        '''
        SEARCH
        '''
        open_expand_form = WebDriverWait(driver, 5).until(ex_cond.presence_of_element_located((
            By.CLASS_NAME, 'open_expand_form')))
        if open_expand_form:
            open_expand_form.click()
        input_search = driver.find_element(By.XPATH, '//input[contains(@type, "text")]')
        if input_search:
            input_search.send_keys(id_db)
        val = driver.find_element(By.CLASS_NAME, 'value')
        if val:
            val.click()
            options = WebDriverWait(driver, 5).until(ex_cond.presence_of_element_located((
                By.CLASS_NAME, 'options')))
            if options:
                lis = options.find_elements(By.TAG_NAME, 'li')
                for li in lis:
                    name = li.text.lower()
                    if name == 'лизинг':
                        li.click()
                        break
        but_submit = driver.find_element(By.XPATH, '//button[contains(@type, "submit")]')
        if but_submit:
            but_submit.click()

        '''
        GET CARD
        '''
        card_link = WebDriverWait(driver, 5).until(ex_cond.presence_of_element_located((
            By.XPATH, '//div[contains(@class, "encumbrances-result__body")]')))
        if card_link:
            link = card_link.find_element(By.TAG_NAME, 'a').get_attribute('href')
            driver.get(link)

            '''
            WORK WITH CARD
            '''
            operation = ''
            subject = WebDriverWait(driver, 5).until(ex_cond.presence_of_element_located((
                By.CLASS_NAME, 'subject')))
            if subject:
                operation = subject.find_element(By.TAG_NAME, 'h2').text.split(' ')[0].lower()
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

            spans = subject.find_elements(By.TAG_NAME, 'span')
            if spans:
                date_publish = spans[-1].text.split(' ')[1]

            cards = driver.find_elements(By.CLASS_NAME, 'card-section')
            if cards:
                '''
                    ПУБЛИКАТОР
                '''
                publisher = cards[0].find_element(By.CLASS_NAME, 'main')
                if publisher:
                    bp = get_bp(publisher)
                    publisher_name = bp[0]
                    publisher_inn = bp[1]
                    publisher_ogrn = bp[2]

                '''
                TABLE CARDS
                '''
                fields = cards[1].find_elements(By.CLASS_NAME, 'field')
                if fields:
                    i = 0
                    j = 1
                    if len(fields) == 5:
                        i = 1
                        j = 2
                        vals = get_main_dog(fields)
                        dogovor_main_real_id = vals[0]
                        dogovor_main_url = vals[1]
                        date_add = time.strftime('%Y-%m-%d %H:%M:%S')
                    if len(fields) == 6:
                        i = 3
                        j = 4
                        vals = get_main_dog(fields)
                        dogovor_main_real_id = vals[0]
                        dogovor_main_url = vals[1]
                        dogovor_stop_date = fields[1].find_element(By.CLASS_NAME, 'field-value').text
                        reason_stop = fields[2].find_element(By.CLASS_NAME, 'field-value').text

                    dog = fields[i].find_element(By.CLASS_NAME, 'field-value')
                    if dog:
                        vals = dog.find_elements(By.TAG_NAME, 'span')
                        if vals:
                            dogovor = vals[0].text
                            dogovor_date = vals[1].text.split(' ')[1]
                    if status == 'sign' or status == 'change':
                        srok_arendy = fields[j].find_element(By.CLASS_NAME, 'field-value')
                        if srok_arendy:
                            period = srok_arendy.text.replace(' ', '')
                            period_start = period.split('-')[0]
                            period_end = period.split('-')[1]
                try:
                    com_el = cards[1].find_element(By.XPATH, '//div[contains(text(), "Комментарий")]')
                    if com_el:
                        div = com_el.find_element(By.XPATH, '..')
                        comments = div.find_element(By.TAG_NAME, 'span').text
                except NoSuchElementException:
                    pass

                '''
                TABLE CARD_LESSORS
                '''
                field_lessors = cards[1].find_element(By.XPATH, '//div[contains(@class, "lessees")]')  # lessors
                if field_lessors:
                    lessor = field_lessors.find_element(By.CLASS_NAME, 'field-value')
                    if lessor:
                        bp = get_bp(lessor)
                        lessor_name = bp[0]
                        lessor_inn = bp[1]
                        lessor_ogrn = bp[2]

                '''
                TABLE CARD_LESSEES
                '''
                fields_lessees = cards[1].find_element(By.XPATH, '//div[contains(@class, "lessors")]')  # lessees
                if fields_lessees:
                    lessees = fields_lessees.find_element(By.CLASS_NAME, 'field-value')
                    if lessees:
                        bp = get_bp(lessees)
                        lessees_name = bp[0]
                        lessees_inn = bp[1]
                        lessees_ogrn = bp[2]

                '''
                TABLE CARD_OBJECTS
                '''
                if status == 'sign' or status == 'change':
                    table = cards[1].find_element(By.CLASS_NAME, 'info_table')
                    if table:
                        table_body = table.find_element(By.TAG_NAME, 'tbody')
                        if table_body:
                            rows = table_body.find_elements(By.TAG_NAME, 'tr')
                            for row in rows:
                                cols = row.find_elements(By.TAG_NAME, 'td')
                                object_name = cols[0].text
                                object_class = cols[1].find_element(By.TAG_NAME, 'span').text
                                object_description = cols[2].text.rstrip()
                                object_total = f'{object_name} {object_class} {object_description}'

    except NoSuchElementException as ex:
        reason = "selen_fill_data_Элемент не найден"
        secure.log.write_log(reason, ex)
        done = 1
        pass
    except WebDriverException as ex:
        print(ex)
        change_proxy(connection, driver, ex, id_db)
        done = 1
        pass
    finally:
        if driver:
            driver.close()
            driver.quit()
            print("[INFO] Selen driver closed")

    if status == 'sign':
        insert_sign_cards(connection, url, real_id, period, dogovor, dogovor_date, date_publish, type_card,
                          period_start, period_end, comments, done)
    elif status == 'change':
        insert_change_cards(connection, url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url,
                            dogovor_date, date_publish, type_card, period_start, period_end, date_add, comments, done)
    elif status == 'stop':
        insert_stop_cards(connection, url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url,
                          reason_stop, dogovor_date, dogovor_stop_date, date_publish, comments, type_card, done)



def get_main_dog(fields):
    vals = []
    dogovor_id = ''
    dogovor_url = ''
    dog = fields[0].find_element(By.CLASS_NAME, 'field-value')
    if dog:
        dogovor_id = dog.find_element(By.TAG_NAME, 'span').text[1:]
        href = dog.find_element(By.TAG_NAME, 'a').get_attribute('href')
        if href:
            dogovor_url = href.split('/')[-1]
    vals.append(dogovor_id)
    vals.append(dogovor_url)
    return vals


def get_bp(data):
    bp = []
    inn = 0
    ogrn = 0
    name = data.text
    spl = name.split('\n')
    if len(spl) == 3:
        name = spl[0]
        inn = spl[1].split(':')[-1]
        ogrn = spl[2].split(':')[-1]
    bp.append(name)
    bp.append(inn)
    bp.append(ogrn)
    return bp


def change_proxy(connection, driver, ex, id_bd):
    reason = "clicked_get_phone _ ОШИБКА ПРОКСИ"
    secure.log.write_log(reason, ex)
    # link = driver.current_url
    if secure.GLOB_ID < 1:
        secure.GLOB_ID += 1
    else:
        secure.GLOB_ID = 0
    print('СМЕНА PROXY')
    secure.log.write_log("СМЕНА PROXY: ", f'new tk.GLOB_ID: {secure.GLOB_ID}')
    time.sleep(600)
    fill_data(connection, id_bd)


def multi_selen(connection, threads_num, ids):
    threads = []
    for i in range(0, threads_num):
        thread = threading.Thread(target=fill_data, args=(connection, ids[i],))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
