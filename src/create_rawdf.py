from pathlib import Path

import pandas as pd
from tqdm.notebook import tqdm


def create_resuls(html_path_list: list[Path]) -> pd.DataFrame:
    """
    raceページのhtmlを読み込んで, レース結果テーブルに加工する関数.
    """
    dfs = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, "rb") as f:
            race_id = html_path.stem  # pathからidのみを取り出せる.
            html = f.read()
            df = pd.read_html(html)[0]
            # 難しい：indexを1レースの行数分指定する必要があるので * len(df)
            df.index = [race_id] * len(df)
            dfs[race_id] = df
    concat_df = pd.concat(dfs.values())  # 複数のテーブルを結合できる.
    concat_df.index.name = "race_id"
    return concat_df
