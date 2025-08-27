import pandas as pd
import sqlite3


# === ЭТАП 1. EXTRACT (извлечение данных) ===
def extract(file_path: str) -> pd.DataFrame:
    """
    Загружает данные из CSV.
    """
    print("=== Шаг 1: Загружаем данные ===")
    df = pd.read_csv(file_path)
    print(f"Загружено {df.shape[0]} строк и {df.shape[1]} колонок")
    return df


# === ЭТАП 2. TRANSFORM (очистка данных) ===
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Очищает и нормализует данные.
    """
    print("=== Шаг 2: Очищаем данные ===")

    # Преобразуем дату
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Заполняем пропуски
    df['text'] = df['text'].fillna("no_text")
    for col in ['comments', 'likes', 'reposts', 'views']:
        df[col] = df[col].fillna(df[col].median()).astype(int)
    df['is_pinned'] = df['is_pinned'].fillna("unknown")
    df['attachments'] = df['attachments'].fillna("unknown")
    df['post_source'] = df['post_source'].fillna("unknown")

    # Дополнительная очистка: убираем строки, где лайков больше, чем просмотров
    df = df[df['likes'] <= df['views']]

    df = df[df['likes'] <= df['views']].copy()
    df['text_len'] = df['text'].apply(lambda x: len(str(x)))

    # Добавляем длину текста
    df['text_len'] = df['text'].apply(lambda x: len(str(x)))



    print(f"После очистки: {df.shape[0]} строк")
    return df


# === ЭТАП 3. LOAD (загрузка в базу) ===
def load(df: pd.DataFrame, db_path: str = "vk_cleaned.db"):
    """
    Загружает очищенные данные в SQLite базу.
    """
    print("=== Шаг 3: Загружаем в базу ===")
    conn = sqlite3.connect(db_path)
    df.to_sql('vk_posts', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Данные сохранены в {db_path} (таблица vk_posts)")

    df = df[df['likes'] <= df['views']].copy()
    df['text_len'] = df['text'].apply(lambda x: len(str(x)))


# === MAIN (запуск всего пайплайна) ===
def main():
    source_file = r"C:\Users\Bulat\Desktop\Project_Data_Analysis\vk_skillbox.csv"
    db_file = "vk_cleaned.db"
    df_raw = extract(source_file)
    df_clean = transform(df_raw)
    load(df_clean, db_file)
    print("=== Пайплайн успешно выполнен ===")


if __name__ == "__main__":
    main()
