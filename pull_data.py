import os
import random
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import re
import logging
import warnings

# 警告非表示
warnings.filterwarnings("ignore")

# ログ設定
LOG_FILE = 'scraping.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# 保存先
OUTPUT_DIR = 'data'

# ユーザーエージェントリスト
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 OPR/85.0.4341.72",
]

# 条件設定
IS_DEBUG = False
IS_CONDITIONAL = False
TARGET_TRACKS = "京都"  # 対象競馬場
TARGET_MONTHS = {4, 5, 10, 11}  # 対象月（春・秋シーズン）

# ユーザーエージェント取得
def get_random_user_agent():
    """ランダムなユーザーエージェントを取得する"""
    return random.choice(USER_AGENTS)

# HTML取得
def fetch_html(url):
    """指定したURLからHTMLを取得する"""
    headers = {'User-Agent': get_random_user_agent()}
    response = requests.get(url, headers=headers)
    response.encoding = "EUC-JP"
    return response.text

# HTML要素の安全取得
def safe_get_text(element, default=""):
    """HTML要素のテキストを安全に取得する"""
    try:
        return element.text.strip() if element else default
    except Exception as e:
        logging.error(f"Error extracting text: {e}")
        return default

# レース条件確認
def is_race_valid(race_info):
    """指定された条件に基づいてレース情報が有効か確認"""
    if not IS_CONDITIONAL:
        return True
    try:
        race_course = race_info.get("course", "")
        race_date = race_info.get("date", "")
        if not race_course or not race_date:
            return False

        # コース条件
        if TARGET_TRACKS not in race_course:
            return False

        # 月条件
        race_month = int(race_date.split("年")[1].split("月")[0])
        if race_month not in TARGET_MONTHS:
            return False

        return True
    except Exception as e:
        logging.error(f"Error validating race conditions: {e}")
        return False

# レース情報の抽出
def extract_race_info(soup):
    """レース情報を抽出"""
    try:
        race_name = safe_get_text(soup.find("dl", attrs={"class": "racedata fc"}).find("h1"), "Unknown Race")
        data_intro = soup.find("div", attrs={"class": "data_intro"})

        if not data_intro:
            logging.error("Data intro section not found.")
            return {"name": race_name, "date": "Unknown Date", "course": "Unknown Course"}

        date_text = safe_get_text(data_intro.find("p"), "Unknown Date").split("/")[0].strip()
        course_text = safe_get_text(data_intro.find("p"), "Unknown Course").split("/")[1].strip()

        return {"name": race_name, "date": date_text, "course": course_text}

    except Exception as e:
        logging.error(f"Error extracting race info: {e}")
        return {"name": "Error", "date": "Error", "course": "Error"}

# レース結果のスクレイピング
def scrape_race_results(race_id_list):
    """レース結果データをスクレイピング"""
    race_results = {}
    for race_id in tqdm(race_id_list):
        time.sleep(3 + random.random() * 5)
        try:
            url = f"https://db.netkeiba.com/race/{race_id}"
            html = fetch_html(url)
            soup = BeautifulSoup(html, "html.parser")

            # レース情報の取得
            race_info = extract_race_info(soup)
            if not is_race_valid(race_info):
                logging.info(f"Race {race_id} does not meet the conditions.")
                continue

            df = pd.read_html(html)[0]
            df = df.rename(columns=lambda x: x.replace(' ', ''))

            horse_id_list = [
                re.findall(r"\d+", a["href"])[0]
                for a in soup.find("table", attrs={"summary": "レース結果"}).find_all("a", attrs={"href": re.compile("^/horse")})
            ]
            jockey_id_list = [
                re.findall(r"\d+", a["href"])[0]
                for a in soup.find("table", attrs={"summary": "レース結果"}).find_all("a", attrs={"href": re.compile("^/jockey")})
            ]

            df["horse_id"] = horse_id_list
            df["jockey_id"] = jockey_id_list
            df.index = [race_id] * len(df)
            race_results[race_id] = df

        except Exception as e:
            logging.error(f"Error scraping race {race_id}: {e}")
            continue

    # 各レースのデータを個別にCSV出力
    for race_id, df in race_results.items():
        try:
            output_path = os.path.join(OUTPUT_DIR, f"race_results_{race_id}.csv")
            df.to_csv(output_path, index=False)
            logging.info(f"Saved race results to {output_path}.")
        except Exception as e:
            logging.error(f"Error saving race results for {race_id}: {e}")

    return race_results

# 馬の過去成績を取得
def scrape_horse_results(horse_id_list):
    """馬の過去成績をスクレイピング"""
    for horse_id in tqdm(horse_id_list):
        time.sleep(3 + random.random() * 5)
        try:
            url = f"https://db.netkeiba.com/horse/{horse_id}"
            html = fetch_html(url)
            df = pd.read_html(html)[2]
            output_path = os.path.join(OUTPUT_DIR, f"horse_results_{horse_id}.csv")
            df.to_csv(output_path, index=False)
            logging.info(f"Saved horse results to {output_path}.")
        except Exception as e:
            logging.error(f"Error scraping horse {horse_id}: {e}")
            continue

# 血統データを取得
def scrape_peds(horse_id_list):
    """血統データをスクレイピング"""
    for horse_id in tqdm(horse_id_list):
        time.sleep(3 + random.random() * 5)
        try:
            url = f"https://db.netkeiba.com/horse/ped/{horse_id}"
            html = fetch_html(url)
            df = pd.read_html(html)[0]
            output_path = os.path.join(OUTPUT_DIR, f"peds_{horse_id}.csv")
            df.to_csv(output_path, index=False)
            logging.info(f"Saved pedigree data to {output_path}.")
        except Exception as e:
            logging.error(f"Error scraping pedigree for horse {horse_id}: {e}")
            continue

# メイン関数
def main():
    """メイン関数"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # レースIDリストを生成
    race_id_list = [
        f"2024{place:02}{kai:02}{day:02}{r:02}"
        for place in range(1, 11)
        for kai in range(1, 7)
        for day in range(1, 13)
        for r in range(1, 13)
    ]

    race_id_list = race_id_list[:10] if IS_DEBUG else race_id_list

    # レース結果を取得
    race_results = scrape_race_results(race_id_list)

    # 馬IDリストを取得（例としてユニークなIDを抽出）
    race_results_df = pd.concat(race_results.values())
    horse_id_list = race_results_df["horse_id"].unique()

    # 馬の過去成績を取得
    scrape_horse_results(horse_id_list)

    # 血統データを取得
    scrape_peds(horse_id_list)


def continue_main():
    """続きから実行"""
    horce_id_list = [os.path.basename(f).split("_")[2].split(".")[0] for f in os.listdir(OUTPUT_DIR) if "horse_results" in f]
    current_horce_id_list = [os.path.basename(f).split("_")[1].split(".")[0] for f in os.listdir(OUTPUT_DIR) if "peds" in f]
    horce_id_list = list(set(horce_id_list) - set(current_horce_id_list))
    scrape_peds(horce_id_list)

if __name__ == "__main__":
    # main()
    continue_main()
