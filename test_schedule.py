import time
import re
import pandas as pd
from datetime import date
from bs4 import BeautifulSoup
from loguru import logger
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import psycopg2
# from sqlalchemy import create_engine
import schedule
from get_debitors import Debitor
from get_all_notification import Lot
import random


def ever_run_script():
    """
    функция, которая запускается в планировщике schedule и собирает данные о торгах в БД
    :return:
    """
    logger.info(f'Скрипт запущен')
    main_list_notifications = []  # список, куда будем складывать все объявления со страницы ТОРГИ

    # options for browser
    options = webdriver.ChromeOptions()
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                         ' Chrome/92.0.4515.131 Safari/537.36')
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.headless = True

    #  main url for run script
    main_url = 'https://bankrot.fedresurs.ru/'

    driver = webdriver.Chrome(
        executable_path='C:\\Users\\iakovalenko_bank\\PycharmProjects\\efrsb\\chromedriver.exe',
        options=options
    )

    driver.get(main_url)
    WebDriverWait(driver, 15).until(EC.text_to_be_present_in_element((By.ID, 'ctl00_lnkTrades'), 'ТОРГИ'))

    # кликаем по пункту меню ТОРГИ
    driver.find_element_by_id('ctl00_lnkTrades').click()

    # получаем HTML код страницы и передаем его в парсер
    page = driver.page_source
    get_all_notification(page, main_list_notifications)

    # в цикле кликаем по кнопкам паггинации. на каждой итерации получаем HTML код страницы и парсим его
    for btn in range(2, 11):
        pagination_buttons = driver.find_element_by_class_name('pager').find_element_by_tag_name('tbody') \
            .find_element_by_tag_name('tr')

        pagination_buttons.find_element_by_xpath(f'./td[{str(btn)}]/a').click()
        time.sleep(2)
        page = driver.page_source
        get_all_notification(page, main_list_notifications)

    driver.close()
    driver.quit()

    # вставляем данные в MAIN_TABLE и удаляем дубликаты строк
    insert_to_main_table(main_list_notifications)

    logger.info(f'ok')


def get_all_notification(html, lst):
    """
    функция собирает все объявления со страницы ТОРГИ
    :param html: HTML код страницы ТОРГИ
    :param lst: список, в который складываются параметры каждого объявления в виде словарей
    :return:
    """
    soup = BeautifulSoup(html, 'lxml')
    list_sales = soup.find('table', class_='bank').find_all('tr')[1:21]
    for sale in list_sales:
        sales_dict = {}
        # get all data to dictionary
        number = sale.find_all('td')[0].text.strip()
        date_sales = sale.find_all('td')[1].text.strip()
        date_add = sale.find_all('td')[2].text.strip()
        place_name = sale.find_all('td')[3].find('a').text.strip()
        place_url = 'https://bankrot.fedresurs.ru' + sale.find_all('td')[3].find('a').get('href')
        debitor_name = sale.find_all('td')[4].find('a').text.strip()
        debitor_url = 'https://bankrot.fedresurs.ru' + sale.find_all('td')[4].find('a').get('href')
        type_sales = sale.find_all('td')[5].find('a').text.strip()
        type_sales_url = 'https://bankrot.fedresurs.ru' + sale.find_all('td')[5].find('a').get('href')
        form_offer = sale.find_all('td')[6].text.strip()
        status = sale.find_all('td')[7].text.strip()

        sales_dict['number'] = number
        sales_dict['date'] = date_sales
        sales_dict['date_add'] = date_add
        sales_dict['place_name'] = place_name
        sales_dict['place_url'] = place_url
        sales_dict['debitor_name'] = debitor_name
        sales_dict['debitor_url'] = debitor_url
        sales_dict['type_sales'] = type_sales
        sales_dict['type_sales_url'] = type_sales_url
        sales_dict['form_offer'] = form_offer
        sales_dict['status'] = status

        lst.append(sales_dict)


def get_new_data(lst):
    list_url = lst
    new_lots = list(extract_data_from_postgresql()['new_sales'])  # список торгов за текущую дату
    new_debitors = extract_data_from_postgresql()['new_debitors']  # получаем список новых должников за прошедшие сутки
    logger.info(f'new debitors - {len(new_debitors)}, new lots - {type(new_lots)}')
    logger.info(f'\nnew debitors: {type(new_debitors)} - {len(new_debitors)}\n'
                f'new lots: {type(new_lots)} - {len(new_lots)}')
    # get_new_debitors(new_debitors)
    # get_new_lots(list_url, new_lots)


def get_new_debitors(new_debitors):
    # полученный список прогоняем в цикле и записываем данные в БД
    while len(new_debitors) > 0:  # список крутится до тех пор, пока в нем имеется хотя бы 1 элемент
        try:  # пытаемся обработать первый элемент списка
            debitor = Debitor()
            debitor = debitor.get_data(new_debitors[0])
            debitor.insert_to_database()
            new_debitors.pop(0)  # если все ок - первый улемент списка удаляется
        except WebDriverException as err:
            logger.error(f'ошибка при получении данных дебитора {err}')
            time.sleep(2400)
            get_new_debitors(new_debitors)


def get_new_lots(list_url, new_lots):
    logger.info(f'new lots - {len(new_lots)}')
    cnt_lots = 0  # счетчик лотов
    while len(new_lots) > 0:
        logger.info(f'{len(new_lots)} -- {new_lots[0]}')
        try:
            time.sleep(random.randint(2, 5))
            lot = Lot()
            current_url_lot_page = lot.get_all_url(new_lots[0])['current_url']  # ссылка на карточку торгов
            debitor_url = lot.get_all_url(new_lots[0])['debitor_url']  # ссылка на url должника

            # проверяем type_sales_url на наличие в контрольном списке. если отсуствует - парсим карточку торгов
            if current_url_lot_page in list_url:
                logger.info(f'iter {cnt_lots+1} -- {current_url_lot_page} also in control list')
                # continue
            else:
                # по ссылке о проведении торгов переходим на страницу и получаем список всех лотов
                lots_list = get_lots_list(current_url_lot_page, debitor_url)
                for lot in lots_list:
                    lot.insert_to_database()
                    logger.info(f'lot insert to DB')
                logger.info(f'check {current_url_lot_page}')
                list_url.append(current_url_lot_page)
            new_lots.pop(0)
            cnt_lots += 1
            logger.info(f'проверили {cnt_lots} лот(ов), осталось проверить {len(new_lots)} лот(ов)')
        except WebDriverException as err:
            logger.error(f'ошибка получения лота {err}')
            time.sleep(2400)
            get_new_lots(list_url, new_lots)


def insert_to_main_table(data):
    """
    функция записывает данные в таблицу MAIN_TABLE и после записи удаляет из нее полные дубликаты строк
    :param data: списсок словарей, полученный при парсинге всех страниц ТОРГИ
    :return:
    """
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
        cur.execute(f"""
            INSERT INTO MAIN_TABLE (
                number, date, date_add, place, place_url, debitor, debitor_url, type_sales,
                type_sales_url, offer_form, status, CurrentDate
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, values)
        con.commit()

    cur.execute(f"""
            DELETE FROM MAIN_TABLE WHERE id NOT IN
                (SELECT max(id) FROM MAIN_TABLE GROUP BY (
                    number, date, date_add, place, place_url, debitor, debitor_url, type_sales,
                    type_sales_url, offer_form, status, CurrentDate))
                """)
    con.commit()
    con.close()


def extract_data_from_postgresql():
    """
    функиця выбирает список url-ов должников за истекшие сутки, список всех должников из БД
    и сравнивает их
    :return: список url-ов должников за истекшие сутки, которые отсуствуют в БД
    """
    con = psycopg2.connect(
        database='efrsb',
        user='postgres',
        password='paedf5l5',
        host='127.0.0.1',
        port='5432'
    )
    query_current_date = 'select * from MAIN_TABLE where currentdate = current_date-3'
    df_current_date = pd.read_sql(query_current_date, con)
    debitors_list_current_date = set(df_current_date['debitor_url'])  # список url-ов должников за текущую дату

    query_all_debitors = 'select * from debitors'
    df_all_debitors = pd.read_sql(query_all_debitors, con)
    all_debitors_list = df_all_debitors['url_efrsb']  # список всех должников из БД
    type_sales_url_list_current_date = df_current_date['type_sales_url']  # список url-ов типов торгов за текущую дату

    con.commit()
    con.close()

    other_debitors_url = list(debitors_list_current_date.difference(all_debitors_list))  # разница списков должников

    return {'new_debitors': other_debitors_url, 'new_sales': type_sales_url_list_current_date}


def get_lots_list(current_url, debitor_url):
    options = webdriver.ChromeOptions()
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                         ' Chrome/92.0.4515.131 Safari/537.36')
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.headless = True
    browser = webdriver.Chrome(
        executable_path='C:\\Users\\iakovalenko_bank\\PycharmProjects\\efrsb\\chromedriver.exe',
        options=options
    )
    lot_list = []

    browser.get(current_url)
    WebDriverWait(browser, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'h1')))
    table_lot_info = browser.find_element_by_class_name('lotInfo').find_elements_by_tag_name('tr')

    for row in table_lot_info[1:]:
        lot = Lot()
        lot.debitor_url = debitor_url
        lot.current_url = current_url
        lot.number = row.find_elements_by_tag_name('td')[0].text.strip()
        lot.description = row.find_elements_by_tag_name('td')[1].text.strip()
        start_price = row.find_elements_by_tag_name('td')[2].text.strip()
        lot.start_price = check_price(start_price)
        step = row.find_elements_by_tag_name('td')[3].text.strip()
        deposit = row.find_elements_by_tag_name('td')[4].text.strip()
        lot.step = check_percent(step)
        lot.deposit = check_percent(deposit)
        lot.class_property = row.find_elements_by_tag_name('td')[-1].text.strip()
        try:
            lot.msg_number = browser.find_element_by_xpath("//td[contains(text(), '№ сообщения')]/../td[last()]") \
                .text.strip()
        except:
            lot.msg_number = None
        try:
            lot.msg_date = browser.find_element_by_xpath("//td[contains(text(),'Дата публикации')]/../td[last()]") \
                .text.strip()
        except:
            lot.msg_date = None
        try:
            lot.place_trade = browser.find_element_by_xpath("//td[contains(text(),'Место проведения:')]/../td[last()]") \
                .text.strip()
        except:
            lot.place_trade = None
        try:
            lot.case_number = browser.find_element_by_xpath("//td[contains(text(),'№ дела')]/../td[last()]") \
                .text.strip()
        except:
            lot.case_number = None
        try:
            lot.start_time = browser.find_element_by_xpath("//td[contains(text(),'Дата и время начала подачи заявок:')]"
                                                           "/../td[last()]") \
                .text.strip()
        except:
            lot.start_time = None
        try:
            lot.end_time = browser.find_element_by_xpath("//td[contains(text(), "
                                                         "'Дата и время окончания подачи заявок:')]/../td[last()]") \
                .text.strip()
        except:
            lot.end_time = None

        lot_list.append(lot)

    return lot_list


def check_percent(txt: str):
    """
    функция принимает на вход строкоывое значение шага торгов и задатка
    и возвращает числовое значение в формате десятичное дроби
    :param txt:
    :return:
    """
    pattern_rub = r"[а-я]+[.,]|[а-я]+"
    pattern_percent = r"[%]"
    pattern_none = r"[-]"

    if re.search(pattern_percent, txt):
        txt = txt.split('%')[0].replace(',', '.')
        percent = float(txt)
        return percent / 100
    if re.search(pattern_none, txt):
        return None
    if re.search(pattern_rub, txt):
        price = re.sub(pattern_rub, '', txt)
        price = price.replace(' ', '').replace(',', '.')
        return float(price)


def check_price(txt: str):
    price = float(txt.replace(' ', '').replace(',', '.'))
    return price


def main():
    list_current_url = []  # список актуальных url-ов карточек торгов для избежания дублей лотов
    #
    # scheduler1 = schedule.Scheduler()
    #
    # scheduler1.every().day.at("07:00").do(ever_run_script)
    # scheduler1.every().day.at("12:00").do(ever_run_script)
    # scheduler1.every().day.at("17:00").do(ever_run_script)
    # scheduler1.every().day.at("22:00").do(ever_run_script)
    # scheduler1.every().day.at("00:10").do(get_new_data, lst=list_current_url)
    # # scheduler1.every().day.at("23:15").do(refresh_trade_place)
    #
    # while True:
    #     scheduler1.run_pending()

    get_new_data(list_current_url)


if __name__ == '__main__':
    main()
