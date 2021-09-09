from loguru import logger
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from sqlalchemy import create_engine


def get_url_notification(url):
    """
    функция принимает на вход url объявления о торгах из карточки торгов
    и возвращает ссылку на страницу с сообщение о торгах
    :param url: url объявления о торгах из карточки торгов
    :return: ссылку на страницу с сообщение о торгах
    """
    driver.get(url)
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'h1')))
    table = driver.find_element_by_id('ctl00_cphBody_tableTradeInfo').find_elements_by_tag_name('tr')
    current_url = table[-1].find_elements_by_tag_name('td')[-1].find_element_by_tag_name('a'). \
        get_attribute('onclick')
    current_url = 'https://bankrot.fedresurs.ru' + current_url.split("'")[1]
    return current_url


df_notifications = pd.DataFrame(columns=['type', 'company_name', 'address', 'inn', 'ogrn_ogrnip', 'last_name',
                                         'first_name', 'father_name', 'date_of_born', 'place_of_born', 'snils',
                                         'previos_name', 'region_court', 'url_efrsb'])
# read url_debitors from BD Postgresql
query_distinct_url = "select distinct(type_sales_url), debitor_url from sales"
engine = create_engine("postgresql+psycopg2://postgres:paedf5l5@127.0.0.1:5432/efrsb")
df_sales_debitors = pd.read_sql(query_distinct_url, engine)
# logger.info(f'\n{df_sales_debitors.head()}')
# print(df_sales_debitors.loc[0]['type_sales_url'])
dict_sales_dibitors = {}
for row in range(5):
    print(df_sales_debitors.loc[row]['type_sales_url'], ' ', df_sales_debitors.loc[row]['debitor_url'])


# logger.info(f'\n{dict_sales_dibitors}')
list_notifications_url = pd.Series(pd.read_sql(query_distinct_url, engine)['type_sales_url']).to_list()

options = webdriver.ChromeOptions()
options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                     ' Chrome/92.0.4515.131 Safari/537.36')
options.add_argument("--disable-blink-features=AutomationControlled")
options.headless = True
driver = webdriver.Chrome(
    executable_path='chromedriver.exe',
    options=options
)

"""
список ссылок на страницы с сообщениями о торгахБ который заполняется в
результате исполнения функции get_url_notification
в дальнейшем список перебирается в цикле и на основе полученной информации
заполняется таблица БД с данными о всех лотах 
"""
URL_NOTIFICATIONS_LIST = []

cnt = 1
for list_element in list_notifications_url[0:5]:
    try:
        url_notification = get_url_notification(url=list_element)
        URL_NOTIFICATIONS_LIST.append(url_notification)
        # logger.info(f'get {url_notification}')
    except Exception as err:
        url_notification = 'отсуствует'
        logger.error(f'url not find {err}')
    # df_debitors = get_all_data(url=url, df=df_debitors)

    cnt += 1

# logger.info(f'{len(URL_NOTIFICATIONS_LIST)}')
# for i in list_notifications_url[:5]:
#     logger.info(f'{i}')
