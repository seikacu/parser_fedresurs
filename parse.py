import aiohttp
import asyncio
import datetime
import time

import secure

from db_sql import insert_change_cards
from db_sql import insert_lessees
from db_sql import insert_lessors
from db_sql import insert_objects
from db_sql import insert_sign_cards
from db_sql import insert_stop_cards


async def change_proxy():
    num_procs = secure.num_proxs
    if secure.PROXY_ID < num_procs - 1:
        secure.PROXY_ID += 1
    else:
        secure.PROXY_ID = 0


def get_date_format(date_str, date_format):
    date = datetime.datetime.strptime(date_str, date_format)
    return date


async def get_card_data(card_dict):
    url = card_dict['guid']
    content = card_dict['content']

    done = 0
    '''
        CARDS
    '''
    type_card = card_dict['messageType']

    status = ''
    if type_card == 'FinancialLeaseContract' or type_card == 'FinancialLeaseContract2':
        status = 'sign'
    elif type_card == 'ChangeFinancialLeaseContract' or type_card == 'ChangeFinancialLeaseContract2':
        status = 'change'
    elif type_card == 'StopFinancialLeaseContract' or type_card == 'StopFinancialLeaseContract2':
        status = 'stop'

    real_id = card_dict['number']
    if 'contractInfo' in content:
        contract_info = content['contractInfo']
        dogovor = contract_info['number']
        dogovor_date = contract_info['date'].split('T')[0]
        dogovor_date = get_date_format(dogovor_date, '%Y-%m-%d')
    else:
        dogovor = content['contractNumber']
        dogovor_date = content['contractDate'].split('T')[0]
        dogovor_date = get_date_format(dogovor_date, '%Y-%m-%d')
    date_publish = card_dict['datePublish'].split('T')[0]
    date_publish = get_date_format(date_publish, '%Y-%m-%d')
    if 'financialLeasePeriod' in content:
        financial_lease_period = content['financialLeasePeriod']
        period, period_end, period_start = await get_period(financial_lease_period)
    else:
        period, period_end, period_start = await get_period(content)

    comments = ''
    if 'text' in content:
        comments = content['text']
        comments = comments.replace("'", '"')
        comments = comments.replace("\n", '')
    """
        CARDS_CHANGE
    """
    dogovor_main_real_id = ''
    dogovor_main_url = ''
    date_add = None
    if 'additionalInfo' in card_dict:
        add_info = card_dict['additionalInfo']['message']
        dogovor_main_real_id = add_info['number']
        dogovor_main_url = add_info['guid']
        date_add = time.strftime('%Y-%m-%d %H:%M:%S')
    """
        CARDS_STOP
    """
    reason_stop = ''
    dogovor_stop_date = None
    if 'stopReason' in content:
        reason_stop = content['stopReason']
        reason_stop = reason_stop.replace("'", '"')
        reason_stop = reason_stop.replace("\n", '')
        dogovor_stop_date = content['stopDate'].split('T')[0]
        dogovor_stop_date = get_date_format(dogovor_stop_date, '%Y-%m-%d')
    '''
        Лизингополучатель
    '''
    lessee_inn, lessee_name, lessee_ogrn = await get_bp(content, 'lessees')
    '''
        Лизингодатель
    '''
    lessor_inn, lessor_name, lessor_ogrn = await get_bp(content, 'lessors')
    '''
        CARD_OBJECTS
    '''
    object_name = ''
    object_class = ''
    object_description = ''
    object_guid = ''
    if status == 'change' and 'changedSubjects' in content and len(content['changedSubjects']) > 0:
        for el in content['changedSubjects']:
            subjects = el
            object_class = subjects['classifier']['code']
            if 'identifier' in subjects:
                object_name = subjects['identifier']
            if 'description' in subjects:
                object_description = subjects['description']
            if 'guid' in subjects:
                object_guid = subjects['guid']
            object_class, object_description, object_name = await fix_object_name(object_class, object_description,
                                                                                  object_name)
            object_total = f'{object_name} {object_class} {object_description}'
            if status == 'sign':
                await insert_objects(url, object_guid, object_name, object_class, object_description, object_total,
                                     'card_objects')
            elif status == 'change':
                await insert_objects(url, object_guid, object_name, object_class, object_description, object_total,
                                     'card_objects_change')
            elif status == 'stop':
                pass
                # await insert_objects(url, object_guid, object_name, object_class, object_description, object_total,
                #                      'card_objects_stop')
    elif 'subjects' in content and len(content['subjects']) > 0:
        for el in content['subjects']:
            subjects = el
            if 'identifier' in subjects:
                object_name = subjects['identifier']
            elif 'subjectId' in subjects:
                object_name = subjects['subjectId']
            if 'classifier' in subjects:
                object_class = subjects['classifier']['code']
            elif 'classifierCode' in subjects:
                object_class = subjects['classifierCode']
            if 'description' in subjects:
                object_description = subjects['description']
            if 'guid' in subjects:
                object_guid = subjects['guid']
            object_class, object_description, object_name = await fix_object_name(object_class, object_description,
                                                                                  object_name)
            object_total = f'{object_name} {object_class} {object_description}'
            if status == 'sign':
                await insert_objects(url, object_guid, object_name, object_class, object_description, object_total,
                                     'card_objects')
            elif status == 'change':
                await insert_objects(url, object_guid, object_name, object_class, object_description, object_total,
                                     'card_objects_change')
            elif status == 'stop':
                pass
                # await insert_objects(url, object_guid, object_name, object_class, object_description, object_total,
                #                      'card_objects_stop')

    if status == 'sign':
        await insert_sign_cards(url, real_id, period, dogovor, dogovor_date, date_publish, type_card, period_start,
                                period_end, comments, done)
        await insert_lessees(url, lessee_name, lessee_inn, lessee_ogrn, 'card_lessees')
        await insert_lessors(url, lessor_name, lessor_inn, lessor_ogrn, 'card_lessors')
    elif status == 'change':
        await insert_change_cards(url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url, dogovor_date,
                                  date_publish, type_card, period_start, period_end, date_add, comments, done)
        await insert_lessees(url, lessee_name, lessee_inn, lessee_ogrn, 'card_lessees_change')
        await insert_lessors(url, lessor_name, lessor_inn, lessor_ogrn, 'card_lessors_change')
    elif status == 'stop':
        await insert_stop_cards(url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url, reason_stop,
                                dogovor_date, dogovor_stop_date, date_publish, comments, type_card, done)
        await insert_lessees(url, lessee_name, lessee_inn, lessee_ogrn, 'card_lessees_stop')
        await insert_lessors(url, lessor_name, lessor_inn, lessor_ogrn, 'card_lessors_stop')


async def fix_object_name(object_class, object_description, object_name):
    object_name = object_name.replace("'", '"')
    object_name = object_name.replace('\n', '')
    object_class = object_class.replace('\n', '')
    object_class = object_class.replace("'", '"')
    object_description = object_description.replace('\n', '')
    object_description = object_description.replace("'", '"')
    return object_class, object_description, object_name


async def get_period(content):
    period = ''
    period_end = None
    period_start = None
    if 'startDate' in content:
        start_period = content['startDate'].split('T')[0]
        period_start = get_date_format(start_period, '%Y-%m-%d')
        end_period = content['endDate'].split('T')[0]
        period_end = get_date_format(end_period, '%Y-%m-%d')
        start_period = str(start_period).split('-')
        start_period = f'{start_period[2]}.{start_period[1]}.{start_period[0]}'
        end_period = str(end_period).split('-')
        end_period = f'{end_period[2]}.{end_period[1]}.{end_period[0]}'
        period = f'{start_period}-{end_period}'
    return period, period_end, period_start


async def get_bp(content, type_bp):
    name = ''
    inn = ''
    ogrn = ''
    fullname = ''
    bp_inn = 0
    bp_ogrn = 0
    if type_bp in content and len(content[type_bp]) > 0:
        name = type_bp
        if 'NonResidentCompany' in content[type_bp][0]['type']:
            # inn = 'regnum'
            bp_inn = '1111111111'
            fullname = 'name'
        else:
            if 'inn' in content[type_bp][0]:
                inn = 'inn'
            if 'ogrn' in content[type_bp][0]:
                ogrn = 'ogrn'
            elif 'ogrnip' in content[type_bp][0]:
                ogrn = 'ogrnip'
            if 'fullName' in content[type_bp][0]:
                fullname = 'fullName'
            elif 'fio' in content[type_bp][0]:
                fullname = 'fio'
    elif f'{type_bp}Companies' in content and len(content[f'{type_bp}Companies']) > 0:
        name = f'{type_bp}Companies'
        inn = 'inn'
        ogrn = 'ogrn'
        fullname = 'fullName'
    elif f'{type_bp}IndividualEntrepreneurs' in content and len(content[f'{type_bp}IndividualEntrepreneurs']) > 0:
        name = f'{type_bp}IndividualEntrepreneurs'
        inn = 'inn'
        ogrn = 'ogrnip'
        fullname = 'fio'
    elif f'{type_bp}Persons' in content and len(content[f'{type_bp}Persons']) > 0:
        name = f'{type_bp}Persons'
        inn = 'inn'
        fullname = 'fio'
    elif f'{type_bp}NonResidentCompanies' in content and len(content[f'{type_bp}NonResidentCompanies']) > 0:
        name = f'{type_bp}NonResidentCompanies'
        # inn = 'regnum'
        bp_inn = '1111111111'
        fullname = 'name'
    bp = content[name][0]
    bp_name = bp[fullname]
    if len(inn) > 1 and inn in bp:
        bp_inn = bp[inn]
    if len(ogrn) > 1 and ogrn in bp:
        bp_ogrn = bp[ogrn]
    bp_name = bp_name.replace("'", '"')
    bp_name = bp_name.replace('\n', '')
    return bp_inn, bp_name, bp_ogrn


async def find_cards(ids):
    aiohttp_client = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False), trust_env=True)
    try:
        for num in ids:
            start_time = time.perf_counter()
            await get_data(aiohttp_client, num)
            end_time = time.perf_counter()
            print(f"Elapsed time: {end_time - start_time:.2f} seconds")
    finally:
        await aiohttp_client.close()


async def get_data(aiohttp_client, num):
    max_retries = 5
    retries = 0
    while retries < max_retries:
        try:
            url_find = 'https://fedresurs.ru/backend/encumbrances'
            headers_find = {
                'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "YaBrowser";v="23"',
                'Pragma': 'no-cache',
                'DNT': '1',
                'sec-ch-ua-mobile': '?0',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/116.0.5845.1077 YaBrowser/23.9.1.1077 Yowser/2.5 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Cache-Control': 'no-cache',
                'Referer': f'https://fedresurs.ru/search/encumbrances?offset=0&limit=15&searchString={num}'
                           '&additionalSearchFnp=false&group=Leasing',
                'sec-ch-ua-platform': '"Linux"',
            }
            params_find = {
                'offset': '0',
                'limit': '15',
                'searchString': f'{num}',
                'group': 'Leasing',
            }
            proxy_ip, proxy_port, proxy_user, proxy_pass = await secure.get_proxy_pref(3)
            proxy = f'htpp://{proxy_user}:{proxy_pass}@{proxy_ip}:{proxy_port}'
            r = await aiohttp_client.get(url=url_find, headers=headers_find, params=params_find, proxy=proxy)
            await change_proxy()
            find_dict = await r.json()
            found = find_dict['found']
            if found == 1:
                print(f'The card {num} has been found')
                page_data = find_dict['pageData'][0]
                guid = page_data['guid']
                headers_card = {
                    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                    'Pragma': 'no-cache',
                    'sec-ch-ua-mobile': '?0',
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                  'Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json, text/plain, */*',
                    'Cache-Control': 'no-cache',
                    'Referer': f'https://fedresurs.ru/sfactmessage/{guid}',
                    'sec-ch-ua-platform': '"Linux"',
                }
                url_card = f'https://fedresurs.ru/backend/sfactmessages/{guid}'
                r2 = await aiohttp_client.get(url=url_card, headers=headers_card, proxy=proxy)
                await change_proxy()
                card_dict = await r2.json()
                await get_card_data(card_dict)
            else:
                print(f'The card {num} does not exist')
            break
        except aiohttp.client_exceptions.ContentTypeError as ex:
            retries = await rep_by_ex(ex, max_retries, num, retries, 'Unexpected content type')
        except aiohttp.ClientError as e:
            retries = await rep_by_ex(e, max_retries, num, retries, 'aiohttp.ClientError')
        except asyncio.TimeoutError as ex:
            retries = await rep_by_ex(ex, max_retries, num, retries, 'asyncio.TimeoutError')
    if retries == max_retries:
        print(f'The card {num} does not exist')
        secure.log.write_log(f'The card {num} does not exist', '')


async def rep_by_ex(ex, max_retries, num, retries, reason):
    print(f"Repeat data scrap {num}")
    secure.log.write_log(f"Error: {reason} for num={num}", ex)
    secure.log.write_log(f"Repeat data scrap {num}", '')
    await change_proxy()
    retries += 1
    print(f"Retry {retries}/{max_retries}")
    secure.log.write_log(f"Retry {retries}/{max_retries}", '')
    return retries
