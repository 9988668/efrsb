import time
from loguru import logger
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime, date
import random
from get_debitors import Debitor
from get_all_notification import Lot
import re


def get_marker(html):
    soup = BeautifulSoup(html, 'lxml')
    marker = soup.find('table', class_='bank').find_all('tr')[1].find_all('td')[0].text.strip()
    return marker


def check_url(url):
    con = psycopg2.connect(
        database='efrsb',
        user='postgres',
        password='********',
        host='127.0.0.1',
        port='5432'
    )
    cur = con.cursor()
    # cur.execute("""select url_efrsb from trade_place""")
    # place_trade_url_list = cur.fetchall()
    # logger.info(f'len place_trade - {len(place_trade_url_list)}')
    cur.execute("""select url_efrsb from debitors""")
    debitor_url_list = list(cur.fetchall())
    # logger.info(f'len debitors - {len(debitor_url_list)}')

    con.commit()
    con.close()

    new_debitor_url_list = []
    for u in debitor_url_list:
        new_url = list(u)[0]
        new_debitor_url_list.append(new_url)

    if url in new_debitor_url_list:
        logger.info(f'\ndebitor {url} also in DB')
    else:
        debitor = Debitor()
        logger.info(f'create object class Debitor')
        debitor = debitor.get_data(url)
        # get_all_data(url)
        logger.info(f'\nDEBITOR {url} IS NOT IN DB')
        debitor.insert_to_database()
        logger.info(f'data insert to DB')


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
            lot.msg_number = browser.find_element_by_xpath("//td[contains(text(), '№ сообщения')]/../td[last()]")\
                .text.strip()
        except:
            lot.msg_number = None
        try:
            lot.msg_date = browser.find_element_by_xpath("//td[contains(text(),'Дата публикации')]/../td[last()]")\
                .text.strip()
        except:
            lot.msg_date = None
        try:
            lot.place_trade = browser.find_element_by_xpath("//td[contains(text(),'Место проведения:')]/../td[last()]")\
                .text.strip()
        except:
            lot.place_trade = None
        try:
            lot.case_number = browser.find_element_by_xpath("//td[contains(text(),'№ дела')]/../td[last()]")\
                .text.strip()
        except:
            lot.case_number = None
        try:
            lot.start_time = browser.find_element_by_xpath("//td[contains(text(),'Дата и время начала подачи заявок:')]"
                                                           "/../td[last()]")\
            .text.strip()
        except:
            lot.start_time = None
        try:
            lot.end_time = browser.find_element_by_xpath("//td[contains(text(), "
                                                         "'Дата и время окончания подачи заявок:')]/../td[last()]")\
            .text.strip()
        except:
            lot.end_time = None

        lot_list.append(lot)

    return lot_list


def get_all_notification(html, list_url):
    all_sales = []
    """
    формируем пустой список, который будет служить 
    для отсеивания дубликотов URL-ов на карточки торгов
    """

    soup = BeautifulSoup(html, 'lxml')
    list_sales = soup.find('table', class_='bank').find_all('tr')[1:21]
    for sale in list_sales:
        lot = Lot()
        sales_dict = {}
        # get all data to dictionary
        number = sale.find_all('td')[0].text.strip()
        date_sales = sale.find_all('td')[1].text.strip()
        place_name = sale.find_all('td')[3].find('a').text.strip()
        place_url = 'https://bankrot.fedresurs.ru' + sale.find_all('td')[3].find('a').get('href')
        debitor_name = sale.find_all('td')[4].find('a').text.strip()
        debitor_url = 'https://bankrot.fedresurs.ru' + sale.find_all('td')[4].find('a').get('href')
        type_sales = sale.find_all('td')[5].find('a').text.strip()
        type_sales_url = 'https://bankrot.fedresurs.ru' + sale.find_all('td')[5].find('a').get('href')
        form_offer = sale.find_all('td')[6].text.strip()
        status = sale.find_all('td')[7].text.strip()

        # check place_url, debitor_url: is locate inside tables place_trade, debitors???
        check_url(debitor_url)
        current_url_lot_page = lot.get_all_url(type_sales_url)['current_url']

        # проверяем type_sales_url на наличие в контрольном списке. если отсуствует - парсим карточку торгов
        if current_url_lot_page in list_url:
            logger.info(f'type sales url also in control list')
        else:
            # по ссылке о проведении торгов переходим на страницу и получаем список всех лотов
            lots_list = get_lots_list(current_url_lot_page, debitor_url)
            for lot in lots_list:
                lot.insert_to_database()
                logger.info(f'lot insert to DB')
            list_url.append(current_url_lot_page)

        sales_dict['number'] = number
        sales_dict['date'] = date_sales
        sales_dict['place_name'] = place_name
        sales_dict['place_url'] = place_url
        sales_dict['debitor_name'] = debitor_name
        sales_dict['debitor_url'] = debitor_url
        sales_dict['type_sales'] = type_sales
        sales_dict['type_sales_url'] = type_sales_url
        sales_dict['form_offer'] = form_offer
        sales_dict['status'] = status

        all_sales.append(sales_dict)

    return all_sales


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
                    type_sales_url, offer_form, status, CurrentDate
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
    fresh_page = driver.page_source
    new_marker = get_marker(fresh_page)
    return new_marker


def main():
    logger.info(f'Скрипт запущен')
    # время первоначального запуска скрипта
    start_script_time = datetime.now()
    # options for browser
    options = webdriver.ChromeOptions()
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                         ' Chrome/92.0.4515.131 Safari/537.36')
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.headless = True
    # main url for run script
    main_url = 'https://bankrot.fedresurs.ru/'
    driver = webdriver.Chrome(
        executable_path='C:\\Users\\iakovalenko_bank\\PycharmProjects\\efrsb\\chromedriver.exe',
        options=options
    )
    cnt_marker = 0  # счетчик количества обновлений таблицы торгов
    cnt_exception = 0  # счетчик ошибок
    list_current_url = []  # список актуальных url-ов карточек торгов для избежания дублей лотов

    driver.get(main_url)
    WebDriverWait(driver, 15).until(EC.text_to_be_present_in_element((By.ID, 'ctl00_lnkTrades'), 'ТОРГИ'))
    # кликаем по пункту меню ТОРГИ
    driver.find_element_by_id('ctl00_lnkTrades').click()
    page = driver.page_source  # получает html-код страницы
    update_marker = get_marker(page)   # получаем значение первого объявления (номер торгов)
    all_notification = get_all_notification(page, list_current_url)   # получаем список всех объявлений со страницы

    logger.info(f'marker - {update_marker}')

    insert_data_in_main_table(all_notification)

    while True:
        # new_marker = refresh_page(min_time=2, max_time=8, driver=driver)
        time.sleep(random.randint(2, 8))
        driver.refresh()
        fresh_page = driver.page_source
        new_marker = get_marker(fresh_page)
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
            logger.info(f'information on page is update\nnew marker - {new_marker}\n')
            insert_data_in_main_table(data=get_all_notification(fresh_page, list_current_url), marker=update_marker)
            update_marker = new_marker


if __name__ == '__main__':
    main()
