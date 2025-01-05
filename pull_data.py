import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import date, datetime, timedelta
import os

# 定数設定
BASE_URL = "https://db.netkeiba.com"
TARGET_YEAR = 2024
TARGET_TRACKS = "京都"  # 対象競馬場
SEASONS = range(6, 9)   # 季節条件
LOG_FILE = "scraping.log"
OUTPUT_DIR = "race_data"  # データ保存先
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
IS_DEBUG = False  # デバッグフラグ
DAYS_TO_FETCH = 365 if not IS_DEBUG else 7  # 収集する日数

# ディレクトリ作成
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ログ設定
logging.basicConfig(
    level=logging.DEBUG if IS_DEBUG else logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler() if IS_DEBUG else logging.NullHandler()
    ]
)

# レース条件を設定できる関数
def is_race_valid(race_info, track, season):
    try:
        logging.debug(f"Validating race info: {race_info}")
        if len(race_info) < 7:
            logging.error(f"Insufficient race info: {race_info}")
            return False

        race_course = race_info[6]
        race_month = int(datetime.strptime(race_info[5], "%Y年%m月%d日").month)  # 月を取得

        if track not in race_course:
            logging.info(f"Race excluded due to track condition: {race_course}")
            return False

        if race_month not in season:
            logging.info(f"Race excluded due to season condition: {race_month}")
            return False

        return True
    except Exception as e:
        logging.error(f"Error validating race conditions: {e}")
        return False

# レースIDの取得
def get_raceid_list_from_date(target_date):
    date_str = f'{target_date.year:04}{target_date.month:02}{target_date.day:02}'
    url = f'{BASE_URL}/race/list/{date_str}'
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        logging.error(f"Error fetching race list for {date_str}: {response.status_code}")
        return []

    response.encoding = "EUC-JP"
    soup = BeautifulSoup(response.text, "html.parser")
    race_list = soup.find('div', attrs={"class": 'race_list fc'})

    if race_list is None:
        logging.warning(f"No races found for {date_str}. Response content: {response.text[:500]}...")
        return []

    try:
        a_tag_list = race_list.find_all('a')
        href_list = [a_tag.get('href') for a_tag in a_tag_list if a_tag.get('href')]
        race_id_list = [re.findall(r'\d{12}', href)[0] for href in href_list if re.findall(r'\d{12}', href)]
        logging.debug(f"Extracted race IDs: {race_id_list}")
        return list(set(race_id_list))
    except Exception as e:
        logging.error(f"Error parsing race list for {date_str}: {e}")
        return []

# レース情報の取得
def get_race_info(soup):
    try:
        race_name = soup.find("dl", attrs={"class": "racedata fc"}).find('h1').text.strip()
        data_intro = soup.find("div", attrs={"class": "data_intro"})
        if data_intro is None:
            logging.error("Data intro section not found.")
            return []

        race_info_list = [race_name]
        p_elements = data_intro.find_all("p")
        if len(p_elements) < 2:
            logging.error("Insufficient data in data_intro section.")
            return []

        race_info_list += p_elements[0].find('span').text.replace('\xa0', '').split('/')
        race_info_list += p_elements[1].text.replace('\xa0', '').split(' ')
        return race_info_list
    except Exception as e:
        logging.error(f"Error extracting race info: {e}")
        return []

# 出走する各馬の情報を取得
def get_horse_info(html, race_id):
    try:
        df = pd.read_html(html.text)[0]
        df.index = [race_id] * len(df)
        soup = BeautifulSoup(html.text, 'html.parser')
        df["horse_id"] = get_idlist_from_table(soup, 'horse')
        df["jockey_id"] = get_idlist_from_table(soup, 'jockey')
        return df
    except Exception as e:
        logging.error(f"Error extracting horse info for race {race_id}: {e}")
        return pd.DataFrame()

# IDリストの抽出
def get_idlist_from_table(soup, target):
    try:
        atag_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
            "a", attrs={"href": re.compile(f"^/{target}")}
        )
        return [re.findall(r"\d+", atag["href"])[0] for atag in atag_list]
    except Exception as e:
        logging.error(f"Error extracting {target} IDs: {e}")
        return []

# 払い戻し情報の取得
def get_return_table(html, race_id):
    try:
        tables = pd.read_html(html.text.replace('<br />', 'sep'))
        return_df = pd.concat(tables[1:3])
        return_df.index = [race_id] * len(return_df)
        return return_df
    except Exception as e:
        logging.error(f"Error extracting return table for race {race_id}: {e}")
        return pd.DataFrame()

# 包括的なレースデータ収集
def scraping_race_table(race_id, track, season):
    url = f"{BASE_URL}/race/{race_id}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        logging.error(f"Error fetching race {race_id}: {response.status_code}")
        return pd.DataFrame(), pd.DataFrame(), []

    response.encoding = "EUC-JP"
    soup = BeautifulSoup(response.text, "html.parser")

    race_info = get_race_info(soup)
    if not is_race_valid(race_info, track, season):
        return pd.DataFrame(), pd.DataFrame(), []

    main_df = get_horse_info(response, race_id)
    return_df = get_return_table(response, race_id)
    return main_df, return_df, race_info

# 使用例
def main():
    race_id_list = []
    start_date = date(TARGET_YEAR, 1, 1)

    for day_offset in range(DAYS_TO_FETCH):
        target_date = start_date + timedelta(days=day_offset)
        race_id_list += get_raceid_list_from_date(target_date)
        time.sleep(1)

    logging.info(f"Collected {len(race_id_list)} race IDs.")

    for race_id in race_id_list:
        main_df, return_df, race_info = scraping_race_table(race_id, TARGET_TRACKS, SEASONS)

        # データを保存
        if not main_df.empty:
            main_df.to_csv(os.path.join(OUTPUT_DIR, f"horse_info_{race_id}.csv"), index=False, encoding="utf-8-sig")
        if not return_df.empty:
            return_df.to_csv(os.path.join(OUTPUT_DIR, f"return_info_{race_id}.csv"), index=False, encoding="utf-8-sig")
        if race_info:
            with open(os.path.join(OUTPUT_DIR, "race_info.csv"), "a", encoding="utf-8-sig") as f:
                f.write(f"{race_id}," + ",".join(race_info) + "\n")

        time.sleep(1)

if __name__ == "__main__":
    main()
