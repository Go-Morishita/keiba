import re  # 正規表現
import time
import traceback
from urllib.request import Request, urlopen

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from tqdm.notebook import tqdm
from webdriver_manager.chrome import ChromeDriverManager


def scrape_kaisai_date(from_: str, to_: str) -> list[str]:
    """
    from_とto_はyyyy-mmの形で指定すると, 間の開催日一覧を取得する関数.
    """

    kaisai_date_list = []

    for date in tqdm(pd.date_range(from_, to_, freq="MS")):
        year = date.year
        month = date.month
        url = f"https://race.netkeiba.com/top/calendar.html?year={year}&month={month}"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}
        req = Request(url, headers=headers)
        html = urlopen(req).read()

        time.sleep(1)

        soup = BeautifulSoup(html, 'html.parser')

        a_list = soup.find("table", class_="Calendar_Table").find_all("a")

        for a in a_list:
            kaisai_date = re.findall(r"kaisai_date=(\d{8})", a["href"])[0]
            kaisai_date_list.append(kaisai_date)

    return kaisai_date_list


def scrape_race_id_list(kaisai_date_list: list[str]) -> list[str]:
    """
    開催日（yyymmdd形式）をリストで入れると, レースid一覧が帰ってくる関数.
    """
    options = Options()
    options.add_argument("--headless")  # 処理軽量化のためにバックグラウンドで実行
    driver_path = ChromeDriverManager().install()
    race_id_list = []

    # for文終了時にwith構文自動的にdriverがquitする.
    with webdriver.Chrome(service=Service(driver_path), options=options) as driver:
        for kaisai_date in tqdm(kaisai_date_list):
            url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={kaisai_date}"
            try:
                driver.get(url)
                time.sleep(1)
                li_list = driver.find_elements(
                    By.CLASS_NAME, "RaceList_DataItem")
                for li in li_list:
                    href = li.find_element(
                        By.TAG_NAME, "a").get_attribute("href")
                    race_id = re.findall(r"race_id=(\d{12})", href)[0]
                    race_id_list.append(race_id)
            except:
                print(f"stopped at {url}")
                print(traceback.format_exc())  # エラー把握
                break
    return race_id_list
