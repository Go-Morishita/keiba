from urllib.request import Request, urlopen
import pandas as pd
from bs4 import BeautifulSoup

import re  # 正規表現
import time
from tqdm.notebook import tqdm


def scrape_kaisai_date(from_: str, to_: str):
    """
    from_とto_はyyyy-mmの形で指定すると、間の開催日一覧を取得する関数
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
