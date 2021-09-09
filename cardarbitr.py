import pickle
import time
from loguru import logger
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import re


def main():
    logger.info(f'Скрипт запущен')
    url = 'https://kad.arbitr.ru/'
    opts = Options()
    # opts = webdriver.ChromeOptions()
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36")
    opts.add_argument("accept=text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9")
    driver = webdriver.Chrome(executable_path='C:\\Users\\iakovalenko_bank\\PycharmProjects\\efrsb\\chromedriver.exe', options=opts)
    driver.get(url)
    cookies = driver.get_cookies()
    logger.info('get cookies')

    for ck in cookies:
        print(ck)


if __name__ == '__main__':
    main()