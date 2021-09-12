import datetime
from loguru import logger
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import psycopg2


def get_all_data(url):
    driver.get(url)
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'h1')))
    card_title = driver.find_element_by_tag_name('h1').text.strip()
    data_table = driver.find_element_by_class_name('au').find_elements_by_tag_name('tr')
    url_efrsb = str(driver.current_url).split('&attempt')[0]
    if card_title == 'Карточка должника - физического лица':
        type_debitor = 'ФЛ'
        company_name = ''

        try:
            last_name = data_table[0].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            last_name = 'нет данных'
        try:
            first_name = data_table[1].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            first_name = 'нет данных'
        try:
            father_name = data_table[2].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            father_name = 'нет данных'
        try:
            date_of_born = data_table[3].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            date_of_born = 'нет данных'
        try:
            place_of_born = data_table[4].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            place_of_born = 'нет данных'
        try:
            region_court = data_table[5].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            region_court = 'нет данных'
        try:
            inn = data_table[6].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            inn = 'нет данных'
        try:
            ogrn_ogrnip = data_table[7].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            ogrn_ogrnip = 'нет данных'
        try:
            snils = data_table[8].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            snils = 'нет данных'
        try:
            previos_name = data_table[9].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            previos_name = 'нет данных'
        try:
            address = data_table[11].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            address = 'нет данных'

    if card_title == 'Карточка должника - юридического лица':
        type_debitor = 'ЮЛ'
        last_name, first_name, father_name, previos_name = '', '', '', ''
        date_of_born, place_of_born = None, ''
        snils = ''
        try:
            company_name = data_table[1].find_elements_by_tag_name('td')[1].find_element_by_tag_name(
                'span').text.strip()
        except AttributeError:
            company_name = 'нет данных'
        try:
            address = data_table[2].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            address = 'нет данных'
        try:
            region_court = data_table[4].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            region_court = 'нет данных'
        try:
            inn = data_table[6].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            inn = 'нет данных'
        try:
            ogrn_ogrnip = data_table[7].find_element_by_tag_name('span').text.strip()
        except AttributeError:
            ogrn_ogrnip = 'нет данных'
    con = psycopg2.connect(
        database='efrsb',
        user='postgres',
        password='paedf5l5',
        host='127.0.0.1',
        port='5432'
    )
    cur = con.cursor()
    values = tuple([type_debitor, company_name, address, inn, ogrn_ogrnip, last_name, first_name, father_name,
                    place_of_born, snils, previos_name, region_court, url_efrsb, date_of_born])
    logger.info(f'\n{values}')
    query_insert_to_debitors = """
                insert into debitors 
                (type, company_name, address, inn, ogrn_ogrnip, last_name, first_name, 
                 father_name, place_of_born, snils, previos_name, region_court, url_efrsb, date_of_born)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    cur.execute(query_insert_to_debitors, values)

    con.commit()
    con.close()


# время начала работы скрипта
start_time = datetime.datetime.now()
logger.info(f'start {start_time}')

con = psycopg2.connect(
        database='efrsb',
        user='postgres',
        password='paedf5l5',
        host='127.0.0.1',
        port='5432'
    )
cur = con.cursor()

# read url from table sales
cur.execute("""select distinct(debitor_url) from sales""")
url_list_from_sales = set(cur.fetchall())
logger.info(f'{len(url_list_from_sales)}')

# read url from table debitors
cur.execute("""select url_efrsb from debitors""")
url_list_from_debitors = set(cur.fetchall())
logger.info(f'{len(url_list_from_debitors)}')

diff_urls = url_list_from_sales.difference(url_list_from_debitors)
logger.info(f'new url - {len(diff_urls)}')

con.close()

options = webdriver.ChromeOptions()
options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                     'Chrome/92.0.4515.131 Safari/537.36')
options.add_argument("--disable-blink-features=AutomationControlled")
options.headless = True
driver = webdriver.Chrome(executable_path='chromedriver.exe', options=options)

if len(diff_urls) > 0:
    new_url = 1
    old_url = 1
    for url in list(diff_urls):
        try:
            get_all_data(url=list(url)[0])
            logger.info(f'get {new_url} new debitor\n{list(url)[0]}')
            new_url += 1
        except Exception:
            logger.info(f'\nуже в списке - {url}\n{old_url} - old_url')
            old_url += 1
else:
    logger.info(f'do not new debitor_url')

driver.close()
driver.quit()
# con.close()

logger.info(f'total time - {datetime.datetime.now() - start_time}')
# logger.info(f'get {new_url} debitors')
