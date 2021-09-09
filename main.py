import time

from loguru import logger
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import re
import psycopg2
from datetime import datetime, date
import random
import pickle


"""
класс карточка торгов.
url - url самой карточки торгов
place, debitor, manager - словари, где ключ название (имя), 
а значения - ссылка на страницу площадки, должника и управляющего.
notification - только url на карточку конкретного объявления
"""


class CardSales:

    def __init__(self, url=None, place=None, debitor=None, manager=None, notification=None):
        self.url = url
        self.place = place
        self.debitor = debitor
        self.manager = manager
        self.notification = notification


class Debitor:

    def __init__(self, name='Unknown', address='Unknown', inn='Unknown', court_case_number='Unknown',
                 date_of_born='empty', place_of_born='empty', snils='empty', previos_name='empty',
                 ogrn='empty', ogrnip='empty', type='empty'):
        self.name = name
        self.address = address
        self.inn = inn
        self.court_case_number = court_case_number
        self.date_of_born = date_of_born
        self.place_of_born = place_of_born
        self.snils = snils
        self.previos_name = previos_name
        self.ogrn = ogrn
        self.ogrnip = ogrnip
        self.type = type


def get_marker(html):
    soup = BeautifulSoup(html, 'lxml')
    marker = soup.find('table', class_='bank').find_all('tr')[1].find_all('td')[0].text.strip()
    return marker


# функция собирает со страницы ТОРГИ url-ы всех торгов
def get_all_notification(html):
    ALL_SALES = []
    soup = BeautifulSoup(html, 'lxml')
    list_sales = soup.find('table', class_='bank').find_all('tr')[1:21]
    for sale in list_sales:
        sales_dict = {}
        number = sale.find_all('td')[0].text.strip()
        date = sale.find_all('td')[1].text.strip()
        place_name = sale.find_all('td')[3].find('a').text.strip()
        place_url = 'https://bankrot.fedresurs.ru' + sale.find_all('td')[3].find('a').get('href')
        debitor_name = sale.find_all('td')[4].find('a').text.strip()
        debitor_url = 'https://bankrot.fedresurs.ru' + sale.find_all('td')[4].find('a').get('href')
        type_sales = sale.find_all('td')[5].find('a').text.strip()
        type_sales_url = 'https://bankrot.fedresurs.ru' + sale.find_all('td')[5].find('a').get('href')
        form_offer = sale.find_all('td')[6].text.strip()
        status = sale.find_all('td')[7].text.strip()
        # ALL_SALES.append(type_sales_url)
        sales_dict['number'] = number
        sales_dict['date'] = date
        sales_dict['place_name'] = place_name
        sales_dict['place_url'] = place_url
        sales_dict['debitor_name'] = debitor_name
        sales_dict['debitor_url'] = debitor_url
        sales_dict['type_sales'] = type_sales
        sales_dict['type_sales_url'] = type_sales_url
        sales_dict['form_offer'] = form_offer
        sales_dict['status'] = status

        ALL_SALES.append(sales_dict)
        # logger.info(f'{sales_dict}')
    return ALL_SALES


def insert_data_in_main_table(data, marker=''):
    con = psycopg2.connect(
        database='efrsb',
        user='postgres',
        password='paedf5l5',
        host='127.0.0.1',
        port='5432'
    )
    cur = con.cursor()
    for row in data:
        all_arguments = list(row.values())
        all_arguments.append(date.today())
        values = tuple(all_arguments)
        if marker != list(values)[0]:
            cur.execute(f"""
                INSERT INTO SALES (
                    number, date, trade_place, url_efrsb, debitor, debitor_url, type_sales,
                    type_sales_url, offer_form, status, date_insert_data
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, values)
            logger.info(f'add data in DB efrsb')
            con.commit()
        else:
            logger.info(f'find marker in table notification')
            break
    con.close()
    logger.info(f'DB commit and close')


def refresh_page(min_time: int, max_time: int, driver):
    time.sleep(random.randint(min_time, max_time))
    driver.refresh()
    refresh_page = driver.page_source
    new_marker = get_marker(refresh_page)
    return new_marker


# функция распарсивает страницу каждого аукциона/п.предложения и собирает url-ы конкретных объявлений о торгах
def get_url_notification(html):
    soup = BeautifulSoup(html, 'lxml')
    link = 'https://bankrot.fedresurs.ru' + \
           soup.find('tr', id='ctl00_cphBody_trMessageLink').find_all('td')[1].find('a').get('onclick').split("'")[1]
    return link


def get_data_in_notification(html):
    cardsales = CardSales()
    debitor = Debitor()
    soup = BeautifulSoup(html, 'lxml')

    allarm = soup.find('h1', class_='red_small').text.strip()
    if allarm == re.compile('\s*Объявление о проведении торгов \(аннулировано, заблокировано\)\s*'):
        logger.info(allarm)
        return debitor

    notification = soup.find('tr', id='ctl00_BodyPlaceHolder_trBodyMessage').find('div', class_='containerInfo')
    debitor_table = soup.find('b', text='Должник').find_parent('div').find_next_sibling('table')

    current_date = date.today()

    try:
        debitor.inn = debitor_table.find('td', text=re.compile('\s+ИНН\s+')).find_next_sibling('td').text.strip()
        if len(debitor.inn) == 12:
            debitor.type = 'ФЛ'
            debitor.ogrn = None
            try:
                debitor.date_of_born = debitor_table.find('td',
                                                          text=re.compile('\s+Дата рождения\s+')).find_next_sibling(
                    'td').text.strip()
                # logger.info(f'дата рождения {debitor.date_of_born}')
            except Exception as err:
                logger.error(f'не смогли получить дату рождения {err}')
            try:
                debitor.place_of_born = debitor_table.find('td',
                                                           text=re.compile('\s+Место рождения\s+')).find_next_sibling(
                    'td').text.strip()
                # logger.info(f'место рождения {debitor.place_of_born}')
            except Exception as err:
                logger.error(f'не смогли получить место рождения {err}')
            try:
                debitor.snils = debitor_table.find('td', text=re.compile('\s+СНИЛС\s+')).find_next_sibling(
                    'td').text.strip()
                # logger.info(f'СНИЛС {debitor.snils}')
            except Exception as err:
                logger.error(f'не смогли получить СНИЛС {err}')
            try:
                debitor.ogrnip = debitor_table.find('td', text=re.compile('\s+ОГРНИП\s+')).find_next_sibling(
                    'td').text.strip()
                # logger.info(f'ОГРНИП {debitor.snils}')
            except Exception as err:
                logger.error(f'не смогли получить ОГРНИП {err}')
            try:
                debitor.previos_name = debitor_table.find('td', text=re.compile(
                    '\s+Ранее имевшиеся ФИО\s+')).find_next_sibling('td').text.strip()
                # logger.info(f'Ранее имевшиеся ФИО {debitor.previos_name}')
            except Exception as err:
                logger.error(f'не смогли получить ранее имевшиеся ФИО {err}')
        elif len(debitor.inn) == 10:
            debitor.type = 'ЮЛ'
            debitor.date_of_born = None
            debitor.place_of_born = None
            debitor.snils = None
            debitor.previos_name = None
            try:
                debitor.ogrn = debitor_table.find('td', text=re.compile('\s+ОГРН\s+')).find_next_sibling(
                    'td').text.strip()
            except Exception as err:
                logger.error(f'не смогли получить ОГРН {err}')
        else:
            raise ValueError("incorrect INN")
        # logger.info(f'должник {debitor.type}')
    except Exception as err:
        logger.error(f'не смогли получить ИНН должника {err}')

    try:
        document_list = soup.find('tr', id='ctl00_BodyPlaceHolder_trDocumentList')
    except Exception as err:
        logger.error(f'документы в объявлении отсутствуют {err}')

    try:
        notification_number = notification.find('td', text=re.compile('\s+№ сообщения\s+')).find_next_sibling(
            'td').text.strip()
    except Exception as err:
        logger.error(f'не смогли получить номер сообщения {err}')

    try:
        notification_date = notification.find('td', text=re.compile('\s+Дата публикации\s+')).find_next_sibling(
            'td').text.strip()
    except Exception as err:
        logger.error(f'не смогли получить дату сообщения {err}')

    try:
        debitor.name = debitor_table.find('td', text=re.compile(
            '\s+Наименование должника\s+|\s+ФИО должника\s')).find_next_sibling('td').text.strip()
        # logger.info(f'наименование должника {debitor.name}')
    except Exception as err:
        logger.error(f'не смогли получить наименование должника {err}')

    try:
        debitor.address = debitor_table.find('td',
                                             text=re.compile('\s+Адрес\s+|\sМесто жительства\s+')).find_next_sibling(
            'td').text.strip()
        # logger.info(f'адрес должника {debitor.address}')
    except Exception as err:
        logger.error(f'не смогли получить адрес должника {err}')

    try:
        debitor.court_case_number = notification.find('td', text=re.compile('\s*№ дела\s*')).find_next_sibling(
            'td').text.strip()
        # debitor.court_case_number = debitor_table.find_all('tr')[-1].find_all('td')[-1].text.strip()
        # logger.info(f'№ дела в суде {debitor.court_case_number}')
    except Exception as err:
        logger.error(f'не смогли получить № дела в суде {err}')

    return debitor


def main():
    logger.info(f'Скрипт запущен')
    # время первоначального запуска скрипта
    start_script_time = datetime.now()
    ALL_NOTIFICATION = []
    options = webdriver.ChromeOptions()
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36')
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.headless = True
    main_url = 'https://bankrot.fedresurs.ru/'
    driver = webdriver.Chrome(
        executable_path='C:\\Users\\iakovalenko_bank\\PycharmProjects\\efrsb\\chromedriver.exe',
        options=options
    )
    cnt_marker = 0 # счетчик количества обновлений таблицы торгов
    cnt_exception = 0 # счетчик ошибок

    driver.get(main_url)
    WebDriverWait(driver, 15).until(EC.text_to_be_present_in_element((By.ID, 'ctl00_lnkTrades'), 'ТОРГИ'))
    # кликаем по пункту меню ТОРГИ
    driver.find_element_by_id('ctl00_lnkTrades').click()
    # load cookies to file
    pickle.dump(driver.get_cookies(), open('efrsb_cookies', 'wb'))
    start_parsing_page = datetime.now()
    page = driver.page_source
    update_marker = get_marker(page)   # получаем значение первого объявления (номер торгов)
    all_notification = get_all_notification(page)   # получаем список всех объявлений со страницы

    logger.info(f'marker - {update_marker}')

    insert_data_in_main_table(all_notification)

    while True:
        # new_marker = refresh_page(min_time=2, max_time=8, driver=driver)
        time.sleep(random.randint(2, 8))
        driver.refresh()
        refresh_page = driver.page_source
        new_marker = get_marker(refresh_page)
        if new_marker == update_marker:
            logger.info(
                f'not update.'
                f'\ntotal time - {datetime.now() - start_script_time}'
                f'\ncnt marker - {cnt_marker}'
                f'\ncnt exception - {cnt_exception}'
            )
            continue
        else:
            cnt_marker += 1
            start_parsing_page = datetime.now()
            logger.info(f'information on page is update\nnew marker - {new_marker}\n')
            insert_data_in_main_table(data = get_all_notification(refresh_page), marker=update_marker)
            update_marker = new_marker


    # all_notification = get_all_notification(page)
    end_parsing_page = datetime.now()
    # logger.info(f'собрали {len(all_notification)} url-ов, время { end_parsing_page - start_parsing_page}')
    # driver.close()

    # # в цикле проходим все url-ы торгов и собираем url-ы кокретных объявления на ЕФРСБ
    # for url in all_notification:
    #     try:
    #         driver.get(url)
    #         page_sales = driver.page_source
    #         url_notification = get_url_notification(page_sales)
    #         if url_notification in ALL_NOTIFICATION:
    #             logger.info(f'{url_notification} уже в списке')
    #         else:
    #             ALL_NOTIFICATION.append(url_notification)
    #     except Exception as err:
    #         logger.error(f'не получилось открыть {url} - {err}')
    # logger.info(f'в списке {len(ALL_NOTIFICATION)} url-ов')
    #
    # con = psycopg2.connect(
    #     database='efrsb',
    #     user='postgres',
    #     password='paedf5l5',
    #     host='127.0.0.1',
    #     port='5432'
    # )
    # logger.info(f'соудинение с БД установлено {con}')
    # cur = con.cursor()
    #
    # for notification in ALL_NOTIFICATION:
    #     driver.get(notification)
    #     page_notification = driver.page_source
    #     try:
    #
    #         debitor = get_data_in_notification(page_notification)
    #         logger.info(
    #             f'должник - {debitor.name}\nпредыдущее имя - {debitor.previos_name}\nадрес - {debitor.address}\n'
    #             f'ИНН - {debitor.inn}\nномер дела в суде - {debitor.court_case_number}\nтип должника - {debitor.type}\n'
    #             f'дата рождения - {debitor.date_of_born}\nместо рождения - {debitor.place_of_born}\n'
    #             f'СНИЛС - {debitor.snils}\nОГРНИП - {debitor.ogrnip}\nОГРН - {debitor.ogrn}')
    #         debitor_values = (debitor.name, debitor.address, debitor.inn, debitor.court_case_number,
    #                           debitor.date_of_born, debitor.place_of_born, debitor.snils, debitor.previos_name,
    #                           debitor.ogrn, debitor.ogrnip, debitor.type)
    #         cur.execute(f"""
    #             INSERT INTO DEBITORS (
    #                 debitor_name, address, inn, court_case_number, date_of_born,
    #                 place_of_born, snils, previos_name, ogrn, ogrnip, debitor_type
    #             )
    #             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #         """, debitor_values)
    #         logger.info(f'add data in DB efrsb')
    #     except Exception as err:
    #         con.commit()
    #         con.close()
    #         logger.error(f'DB commit and close {err}')
    #
    # cur.execute("""select COUNT(*) from debitors;""")
    # cnt_str_in_database_before_delete = list(cur.fetchall()[0])[0]
    # logger.info(f'{cnt_str_in_database_before_delete} - {type(cnt_str_in_database_before_delete)}')
    #
    # cur.execute("""
    #     DELETE FROM debitors WHERE id NOT IN (
    #     SELECT MIN(id) FROM debitors GROUP BY inn);
    # """)
    # cur.execute("""select COUNT(*) from debitors;""")
    # cnt_str_in_database_after_delete = list(cur.fetchall()[0])[0]
    # logger.info(f'удалены дубликаты - {cnt_str_in_database_before_delete - cnt_str_in_database_after_delete} шт.')
    # con.commit()
    # logger.info(f'DB commit')
    # con.close()
    # logger.info(f'скрипт отработал, в БД {cnt_str_in_database_after_delete} записей')


if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
