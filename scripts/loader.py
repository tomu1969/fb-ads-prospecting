import re
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
INPUT_FILE = BASE_DIR / "input" / "FB Ad library scraping.xlsx"
OUTPUT_FILE = BASE_DIR / "processed" / "01_loaded.csv"

REAL_ESTATE_KEYWORDS = [
    "real estate", "realtor", "realty", "property", "home", "house",
    "mortgage", "listing", "broker", "agent", "condo", "apartment",
    "housing", "residential", "commercial", "investment property"
]


def load_excel(path: Path = INPUT_FILE) -> pd.DataFrame:
    return pd.read_excel(path)


def normalize_page_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE
    )
    df["page_name"] = (
        df["page_name"]
        .astype(str)
        .apply(lambda x: emoji_pattern.sub("", x))
        .str.strip()
        .str.normalize("NFKC")
    )
    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    platform_cols = [c for c in df.columns if c.startswith("platforms/")]

    def combine_platforms(row):
        platforms = []
        for col in platform_cols:
            val = row[col]
            if pd.notna(val):
                platforms.append(str(val))
        return list(set(platforms))

    df = df.copy()
    df["_platforms"] = df.apply(combine_platforms, axis=1)

    grouped = df.groupby("page_name", as_index=False).agg(
        ad_count=("page_name", "count"),
        total_page_likes=("page_likes", "max"),
        ad_texts=("text", lambda x: list(x.dropna())),
        platforms=("_platforms", lambda x: list(set(p for lst in x for p in lst))),
        is_active=("is_active", "any"),
        first_ad_date=("start_date", "min")
    )
    return grouped


def filter_relevant(df: pd.DataFrame) -> pd.DataFrame:
    df_raw = load_excel()
    df_raw = normalize_page_names(df_raw)

    housing_pages = set(df_raw[df_raw["ad_category"] == "HOUSING"]["page_name"])

    keyword_pattern = "|".join(REAL_ESTATE_KEYWORDS)
    keyword_mask = df_raw["text"].fillna("").str.lower().str.contains(keyword_pattern, regex=True)
    keyword_pages = set(df_raw[keyword_mask]["page_name"])

    relevant_pages = housing_pages | keyword_pages
    return df[df["page_name"].isin(relevant_pages)]


def load_and_process(input_path: Path = INPUT_FILE, output_path: Path = OUTPUT_FILE) -> pd.DataFrame:
    df = load_excel(input_path)
    df = normalize_page_names(df)
    df = deduplicate(df)
    df = filter_relevant(df)

    df["first_ad_date"] = pd.to_datetime(df["first_ad_date"]).dt.strftime("%Y-%m-%d")
    df["ad_texts"] = df["ad_texts"].apply(str)
    df["platforms"] = df["platforms"].apply(str)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")

    return df


if __name__ == "__main__":
    print(f"Loading data from: {INPUT_FILE}")
    df = load_and_process()
    print(f"\nProcessed {len(df)} unique advertisers")
    print(f"Output saved to: {OUTPUT_FILE}")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nSample row:")
    print(df.iloc[0].to_string())
