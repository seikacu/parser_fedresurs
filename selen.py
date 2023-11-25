import threading
import time
import zipfile

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, TimeoutException, \
    WebDriverException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as ex_cond
from fake_useragent import UserAgent

from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

import secure
from db_sql import add_phone1, add_phone2


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


def extract_phone_numbers(connection, driver: webdriver.Chrome, id_db):
    try:
        phone1 = driver.find_element(By.XPATH, "//span[contains(@id, 'phone_td_1')]").text
        if phone1.endswith("***"):
            return False
        add_phone1(connection, id_db, phone1)
    except NoSuchElementException:
        add_phone1(connection, id_db, "Объявление снято с публикации")
        reason = "selen_extract_phone_numbers_ Объявление снято с публикации"
        secure.log.write_log(reason, '')
        pass
    try:
        phone2 = driver.find_element(By.XPATH, "//span[contains(@id, 'phone_td_2')]").text
        add_phone2(connection, id_db, phone2)
    except NoSuchElementException:
        reason = "selen_extract_phone_numbers_Отсутствует 2-ой телефон"
        secure.log.write_log(reason, '')
        pass


def get_phone(connection, driver: webdriver.Chrome, id_bd):
    try:
        try:
            try:
                driver.find_element(By.XPATH, "//span[contains(@id, 'phone_td_1')]")
            except NoSuchElementException:
                add_phone1(connection, id_bd, "Объявление снято с публикации")
                reason = "get_phone Номер телефона отсутствует, и/или объявление снято с публикации"
                secure.log.write_log(reason, '')
                pass
                return
            show_phone = WebDriverWait(driver, 10).until(ex_cond.presence_of_element_located((
                By.XPATH, "//a[contains(@onclick, '_show_phone')]")))
            if show_phone.is_displayed():
                driver.execute_script("arguments[0].click();", show_phone)
                time.sleep(1)
            try:
                # ПРОВЕРКА НА ВСПЛЫВАЮЩЕЕ ОКНО "Вы зашли по неверной ссылке,
                # либо у объявления истёк срок публикации. ss.lv"
                alert = driver.find_element(By.ID, "alert_msg")
                if alert:
                    alert_txt = alert.text
                    if 'Вы зашли по неверной ссылке' in alert_txt:
                        # link = driver.current_url
                        if secure.GLOB_ID < 1:
                            secure.GLOB_ID += 1
                        else:
                            secure.GLOB_ID = 0
                        print('СМЕНА PROXY ПО ALERT')
                        secure.log.write_log("СМЕНА PROXY ПО ALERT: ", f'new tk.GLOB_ID: {secure.GLOB_ID}')
                        time.sleep(600)
                        fill_data(connection, id_bd)
            except NoSuchElementException:
                reason = "selen_get_phone_ Верная ссылка - объявление актуально"
                secure.log.write_log(reason, '')
                pass
            pass
        except TimeoutException as ex:
            reason = ("selen_get_phone_timeout - Кнопка Показать номер телефона отсутствует, и/или объявление снято с "
                      "публикации")
            secure.log.write_log(reason, ex)
            pass
        except NoSuchElementException:
            reason = "selen_get_phone_ Кнопка Показать номер телефона отсутствует, и/или объявление снято с публикации"
            secure.log.write_log(reason, f'Номер id в БД: {id_bd}')
            pass
        time.sleep(1)

        if extract_phone_numbers(connection, driver, id_bd) is False:
            print("ПОВТОР ПОЛУЧЕНИЯ НОМЕРА")
            secure.log.write_log("ПОВТОР ПОЛУЧЕНИЯ НОМЕРА", f'Запись в БД: {id_bd}')
            driver.refresh()
            get_phone(connection, driver, id_bd)
    except NoSuchElementException as ex:
        reason = "selen_get_phone_Элемент не найден"
        secure.log.write_log(reason, ex)
        pass
    except WebDriverException as ex:
        change_proxy(connection, driver, ex, id_bd)
        pass


def fill_data(connection, id_db):
    driver = None
    # link = f'https://fedresurs.ru/search/encumbrances?offset=0&limit=15&searchString={id_db}&group=Leasing'
    # link = 'https://fedresurs.ru/search/encumbrances?offset=0&limit=15&group=Leasing'
    link = 'https://fedresurs.ru/search/encumbrances'
    '''
    ПУБЛИКАТОР - куда его грузить???
    '''
    publisher_name = ''
    publisher_inn = ''
    publisher_ogrn = ''

    '''
    CARDS
    '''
    url = ''
    real_id = id_db
    period = ''
    dogovor = ''
    dogovor_date = ''
    date_publish = ''
    type_name = ''  # FinancialLeaseContract
    period_start = ''
    period_end = ''

    '''
    CARD_LESSEES
    '''
    lessees_name = ''
    lessees_inn = ''
    lessees_ogrn = ''

    '''
    CARD_LESSORS
    '''
    lessor_name = ''
    lessor_inn = ''
    lessor_ogrn = ''

    '''
    CARD_OBJECTS
    '''
    object_name = ''
    object_class = ''
    object_description = ''
    object_total = ''

    try:
        driver = get_selenium_driver(True, secure.GLOB_ID)
        driver.get(link)

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

        card_link = WebDriverWait(driver, 5).until(ex_cond.presence_of_element_located((
            By.XPATH, '//div[contains(@class, "encumbrances-result__body")]')))
        if card_link:
            link = card_link.find_element(By.TAG_NAME, 'a').get_attribute('href')
            driver.get(link)

            status = ''
            opeartion = ''
            subject = WebDriverWait(driver, 5).until(ex_cond.presence_of_element_located((
                By.CLASS_NAME, 'subject')))
            if subject:
                opeartion = subject.find_element(By.TAG_NAME, 'h2').text.split(' ')[0].lower()
            if opeartion == 'заключение':
                status = 'sign'
            elif opeartion == 'изменение':
                status = 'change'
            elif opeartion == 'прекращение':
                status = 'stop'

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
                CARDS
                '''
                fields = cards[1].find_elements(By.CLASS_NAME, 'field')
                if fields:
                    dog = fields[0].find_element(By.CLASS_NAME, 'field-value')
                    if dog:
                        vals = dog.find_elements(By.TAG_NAME, 'span')
                        if vals:
                            dogovor = vals[0].text
                            dogovor_date = vals[1].text.split(' ')[1]
                    srok_arendy = fields[1].find_element(By.CLASS_NAME, 'field-value')
                    if srok_arendy:
                        period = srok_arendy.text.replace(' ', '')
                        period_start = period.split('-')[0]
                        period_end = period.split('-')[1]

                '''
                CARD_LESSEES
                '''
                fields_lessees = cards[1].find_element(By.XPATH, '//div[contains(@class, "lessees")]')
                if fields_lessees:
                    lessees = fields_lessees.find_element(By.CLASS_NAME, 'field-value')
                    if lessees:
                        bp = get_bp(lessees)
                        lessees_name = bp[0]
                        lessees_inn = bp[1]
                        lessees_ogrn = bp[2]

                '''
                CARD_LESSORS
                '''
                field_lessors = cards[1].find_element(By.XPATH, '//div[contains(@class, "field lessors")]')
                if field_lessors:
                    lessor = field_lessors.find_element(By.CLASS_NAME, 'field-value')
                    if lessor:
                        bp = get_bp(lessor)
                        lessor_name = bp[0]
                        lessor_inn = bp[1]
                        lessor_ogrn = bp[2]

                '''
                CARD_OBJECTS
                '''
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

        time.sleep(5)
        # get_phone(connection, driver, id_db)

    except NoSuchElementException as ex:
        reason = "selen_fill_data_Элемент не найден"
        secure.log.write_log(reason, ex)
        # pass
    except WebDriverException as ex:
        print(ex)
        change_proxy(connection, driver, ex, id_db)
        # pass
    finally:
        if driver:
            driver.close()
            driver.quit()
            print("[INFO] Selen driver closed")


def get_bp(data):
    bp = []
    inn = ''
    ogrn = ''
    name = data.text
    spl = name.split('\n')
    if len(spl) == 3:
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
