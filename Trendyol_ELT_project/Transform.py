import pandas as pd
import numpy as np
import sqlite3

def transform_from_csv(df):
    # Assigned '0' to NaN values.
    df = df.rename(columns={'Rating Count': 'Rating_Count'})
    df['Rating_Count'] = df['Rating_Count'].fillna(0)  # Bu işlem eksik verilerin veri tipine dönüşüm sırasında hata yaratmasını önler.
    df['Rating_Count'] = df['Rating_Count'].astype(float)

    df = df.rename(columns={'Rating Score': 'Rating_Score'})
    df['Rating_Score'] = df['Rating_Score'].astype(float)  # Veri türünü değiştirir
    df['Rating_Score'] = df['Rating_Score'].fillna(0)  # Eksik değerleri doldurur

    df['Price'] = (df['Price']
                   .str.replace('TL', '', regex=False)
                   .str.replace('.', '', regex=False)
                   .str.replace(',', '', regex=False)
                   .astype(int))
    df['Price'] = df['Price'].fillna(df['Price'].mean())

    df['Brand'] = df['Brand'].astype(str)
    df['Brand'] = df['Brand'].str.upper()

    df = df[df['Price'] >= 6000]
    df = df[df['Price'] <= 352000]

    usd_to_try = 34.75
    euro_to_try = 36.50
    df['Price_TRY'] = df['Price']
    df['Price_USD'] = [np.around(i / usd_to_try, 2) for i in df['Price_TRY']]
    df['Price_EUR'] = [np.around(i / euro_to_try, 2) for i in df['Price_TRY']]
    df = df.drop(columns=['Price'])

    drop_brands = ['DARK', 'EVEREST', 'FOSILTECH', 'GPTURKGRUP', 'HOMETECH', 'I-LIFE DIGITAL', 'KESEROĞLU',
                   'LIFE', 'MAPLE','SHENZHEN TECHNOLOGY', 'TECHNOPC', 'TECNO','TEKNOMAX', 'WENN','ÖKMENBİLİŞİM']
    df = df[~df['Brand'].isin(drop_brands)]
    return df

def load_to_csv(df, file_path):
    df.to_csv(file_path, index=False)
    print(f'Transform edilmiş veriler {file_path} dosyasına kaydedildi.')

def load_to_db(df, db_name, table_name):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn , if_exists='replace', index=False)
    conn.close()
    print(f"Veriler '{db_name}' veritabanındaki '{table_name}' tablosuna yüklendi.")

def process_data(csv_input_path, csv_output_path, db_name, table_name):
    """It processes the data, transforms and saves it to both CSV and SQLite database."""
    df = pd.read_csv(csv_input_path)
    transformed_df = transform_from_csv(df)
    load_to_csv(transformed_df, csv_output_path)
    load_to_db(transformed_df, db_name, table_name)


# start process
process_data(csv_input_path='trendyol_laptop.csv', csv_output_path='transformed_data_.csv',
             db_name='trendyol_laptop.db', table_name='Laptop'
)



