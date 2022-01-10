from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import psycopg2


class Lot:

    options = webdriver.ChromeOptions()
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                         ' Chrome/92.0.4515.131 Safari/537.36')
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.headless = True
    driver = webdriver.Chrome(
        executable_path='C:\\Users\\iakovalenko_bank\\PycharmProjects\\efrsb\\chromedriver.exe',
        options=options
    )

    def __init__(self, msg_number=None, msg_date=None, number=None, description=None, start_price=None, step=None,
                 deposit=None, class_property=None, debitor_url=None, start_time=None, end_time=None, case_number=None,
                 place_trade=None, current_url=None):
        self.msg_number = msg_number
        self.msg_date = msg_date
        self.number = number
        self.description = description
        self.start_price = start_price
        self.step = step
        self.deposit = deposit
        self.class_property = class_property
        self.debitor_url = debitor_url
        self.start_time = start_time
        self.end_time = end_time
        self.case_number = case_number
        self.place_trade = place_trade
        self.current_url = current_url

    def get_all_url(self, url):
        Lot.driver.get(url)
        WebDriverWait(Lot.driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'h1')))
        self.current_url = 'https://bankrot.fedresurs.ru' + \
                           Lot.driver.find_element(By.XPATH,
                                               ("//td[contains(text(), 'Объявление о торгах в ЕФРСБ')]/../td/b/a"))\
                           .get_attribute('onclick').split("'")[1]
        self.debitor_url = Lot.driver.find_element(By.XPATH, ("//td[contains(text(), 'Должник')]/../td/b/a"))\
            .get_attribute('href')
        return {'current_url': self.current_url, 'debitor_url': self.debitor_url}

    @staticmethod
    def lots_cnt(current_url):
        Lot.driver.get(current_url)
        WebDriverWait(Lot.driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'h1')))
        table_lot_info = Lot.driver.find_element_by_class_name('lotInfo').find_elements_by_tag_name('tr')
        return len(table_lot_info) - 1

    def insert_to_database(self):
        con = psycopg2.connect(
            database='efrsb',
            user='postgres',
            password='********',
            host='127.0.0.1',
            port='5432'
        )
        cur = con.cursor()
        values = tuple([self.msg_number, self.msg_date, self.number, self.description, self.start_price, self.step,
                        self.deposit, self.class_property, self.debitor_url, self.start_time, self.end_time,
                        self.case_number, self.place_trade, self.current_url])
        query = """
                insert into lots
                (msg_number, msg_date, number, description, start_price, step, deposit, class_property, debitor_url,
                start_time, end_time, case_number, place_trade, current_url)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        cur.execute(query, values)

        con.commit()
        con.close()

    # def get_lots_list(self, current_url, driver=browser):
    #     lot_list = []
    #     driver.get(current_url)
    #     WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'h1')))
    #     table_lot_info = driver.find_element_by_class_name('lotInfo').find_elements_by_tag_name('tr')
    #
    #     for lot in table_lot_info[1:]:
    #         self.number = lot.find_element(By.XPATH("./td[1]")).text.strip()
    #         self.description = lot.find_element(By.XPATH("./td[2]")).text.strip()
    #         self.start_price = lot.find_element(By.XPATH("./td[3]")).text.strip()
    #         self.step = lot.find_element(By.XPATH("./td[4]")).text.strip()
    #         self.deposit = lot.find_element(By.XPATH("./td[5]")).text.strip()
    #         self.class_property = lot.find_element(By.XPATH("./td[last()]")).text.strip()
    #         self.msg_number = driver.find_element(
    #             By.XPATH("//td[contains(text(), '№ сообщения')]/../td[last()]")).text.strip()
    #         self.msg_date = driver.find_element(
    #             By.XPATH("//td[contains(text(), 'Дата публикации')]/../td[last()]")).text.strip()
    #         self.place_trade = driver.find_element(
    #             By.XPATH("//td[contains(text(), 'Место проведения:')]/../td[last()]")).text.strip()
    #         self.case_number = driver.find_element(
    #             By.XPATH("//td[contains(text(), '№ дела')]/../td[last()]")).text.strip()
    #         self.start_time = driver.find_element(
    #             By.XPATH("//td[contains(text(), 'Дата и время начала подачи заявок:')]/../td[last()]")).text.strip()
    #         self.end_time = driver.find_element(
    #             By.XPATH("//td[contains(text(), 'Дата и время окончания подачи заявок:')]/../td[last()]")).text.strip()
    #
    #         lot_list.append(self)
    #
    #     return lot_list
