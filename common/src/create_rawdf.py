from pathlib import Path

import pandas as pd
from tqdm.notebook import tqdm
from bs4 import BeautifulSoup

import re

RAWDF_DIR = Path("..", "data", "rawdf")


def create_resuls(html_path_list: list[Path], save_dir: Path = RAWDF_DIR, save_filename: str = "results.csv") -> pd.DataFrame:
    """
    raceページのhtmlを読み込んで, レース結果テーブルに加工する関数.
    """
    dfs = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, "rb") as f:
            race_id = html_path.stem  # pathからidのみを取り出せる.
            html = (
                f.read()
                .replace(b"<diary_snap_cut>", b"")
                .replace(b"</diary_snap_cut>", b"")
            )
            soup = BeautifulSoup(html, "lxml").find(
                "table", class_="race_table_01 nk_tb_common")

            df = pd.read_html(html)[0]

            # horse_id列の追加
            a_list = soup.find_all("a", href=re.compile(r"^/horse/"))
            horse_id_list = []
            for a in a_list:
                horse_id = re.findall(r"\d{10}", a["href"])[0]
                horse_id_list.append(horse_id)
            df["horse_id"] = horse_id_list

            # jockey_id列の追加
            a_list = soup.find_all("a", href=re.compile(r"^/jockey/"))
            jockey_id_list = []
            for a in a_list:
                jockey_id = re.findall(r"\d{5}", a["href"])[0]
                jockey_id_list.append(jockey_id)
            df["jockey_id"] = jockey_id_list

            # trainer_id列の追加
            a_list = soup.find_all("a", href=re.compile(r"^/trainer/"))
            trainer_id_list = []
            for a in a_list:
                trainer_id = re.findall(r"\d{5}", a["href"])[0]
                trainer_id_list.append(trainer_id)
            df["trainer_id"] = trainer_id_list

            # owner_id列の追加
            a_list = soup.find_all("a", href=re.compile(r"^/owner/"))
            owner_id_list = []
            for a in a_list:
                owner_id = re.findall(r"\d{6}", a["href"])[0]
                owner_id_list.append(owner_id)
            df["owner_id"] = owner_id_list

            # 難しい：indexを1レースの行数分指定する必要があるので * len(df)
            df.index = [race_id] * len(df)
            dfs[race_id] = df
    concat_df = pd.concat(dfs.values())  # 複数のテーブルを結合できる.
    concat_df.index.name = "race_id"

    concat_df.columns = concat_df.columns.str.replace(" ", "")
    save_dir.mkdir(parents=True, exist_ok=True)
    concat_df.to_csv(save_dir / save_filename, sep="\t")
    return concat_df


def create_horse_resuls(html_path_list: list[Path], save_dir: Path = RAWDF_DIR, save_filename: str = "horse_results.csv") -> pd.DataFrame:
    """
    horseページのhtmlを読み込んで, 馬の過去成績テーブルに加工する関数.
    """
    dfs = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, "rb") as f:
            horse_id = html_path.stem  # pathからidのみを取り出せる.
            html = f.read()
            df = pd.read_html(html)[2]
            # 難しい：indexを1レースの行数分指定する必要があるので * len(df)
            df.index = [horse_id] * len(df)
            dfs[horse_id] = df
    concat_df = pd.concat(dfs.values())  # 複数のテーブルを結合できる.
    concat_df.index.name = "horse_id"

    concat_df.columns = concat_df.columns.str.replace(" ", "")
    save_dir.mkdir(parents=True, exist_ok=True)
    concat_df.to_csv(save_dir / save_filename, sep="\t")
    return concat_df
