import time
from loguru import logger
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from sqlalchemy import create_engine


def get_data_from_page(df):
    """
    функция получает на вход ДФ, в который дописывает построчно новые данные
    :param df: на вход подается пустой шаблон ДФ с именованными столбцами
    :return: на выходе - обновленный ДФ
    """
    table = driver.find_element_by_class_name('bank').find_elements_by_tag_name('tr')

    for row in table[1:-2]:
        url_efrsb = row.find_elements_by_tag_name('td')[0].find_element_by_tag_name('a').get_attribute('href')
        name = row.find_elements_by_tag_name('td')[0].find_element_by_tag_name('span').text.strip()
        site = row.find_elements_by_tag_name('td')[1].text.strip()
        company_name = row.find_elements_by_tag_name('td')[2].text.strip()
        sro = row.find_elements_by_tag_name('td')[3].find_element_by_tag_name('a').text.strip()
        new_row = pd.Series([name, site, company_name, sro, url_efrsb], index=df.columns)
        df = df.append(new_row, ignore_index=True)

    return df


# create new DataFrame
trade_place = pd.DataFrame(columns=['name', 'site', 'company_name', 'sro', 'url_efrsb'])

url = 'https://bankrot.fedresurs.ru/TradePlaceListWindow.aspx?rwndrnd=0.29609176367185497'
options = webdriver.ChromeOptions()
options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                     '(KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36')
options.add_argument("--disable-blink-features=AutomationControlled")
options.headless = True
driver = webdriver.Chrome(
    executable_path='chromedriver.exe',
    options=options
)

driver.get(url)
WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.ID, 'ctl00_BodyPlaceHolder_TradePlasceList1_btnSearch')))

search_btn = driver.find_element_by_id('ctl00_BodyPlaceHolder_TradePlasceList1_btnSearch')
# logger.info('find search btn')
search_btn.click()
# logger.info('click search btn')
time.sleep(5)

trade_place = get_data_from_page(trade_place)

# get pagination buttons
pagination_btns = driver.find_element_by_class_name('bank')\
                        .find_elements_by_tag_name('tr')[-1]\
                        .find_elements_by_tag_name('td')
actual_page = 0
for btn in range(len(pagination_btns)-1):
    pagination_btns[actual_page+1].click()
    time.sleep(5)
    trade_place = get_data_from_page(trade_place)
    pagination_btns = driver.find_element_by_class_name('bank')\
                            .find_elements_by_tag_name('tr')[-1]\
                            .find_elements_by_tag_name('td')
    actual_page += 1

driver.close()
driver.quit()

# save data to BD Postgresql
engine = create_engine("postgresql+psycopg2://postgres:paedf5l5@127.0.0.1:5432/efrsb")
trade_place.to_sql('trade_place', engine, if_exists='replace')

logger.info(f'get {len(trade_place)} place_trade')
