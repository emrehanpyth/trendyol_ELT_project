import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_from_market(page_number):
    URL = f'https://www.trendyol.com/laptop-x-c103108?pi={page_number}'
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0"}
    r = requests.get(URL)

    if r.status_code == 200:
        html_content = r.text
    else:
        print("Web sitesine bağlanırken bir hata olustu.")

    soup = BeautifulSoup(r.text,'html.parser')

    product_card = soup.find_all('div', class_='p-card-wrppr with-campaign-view')

    product_brand_name = [
        product.find('span',class_='prdct-desc-cntnr-ttl').text
        if product.find('span',class_='prdct-desc-cntnr-ttl') else None
        for product in product_card
    ]
    product_name_list = [
        product.get('title')
        if product.get('title') else None
        for product in product_card
    ]
    product_prices = [
        product.find('div', class_='prc-box-dscntd').text
        if product.find('div', class_='prc-box-dscntd') else None
        for product in product_card
    ]
    product_rating_scores = [
        product.find('span',class_='rating-score').text
        if product.find('span',class_='rating-score') else None
        for product in product_card
    ]
    product_rating_count = [
        product.find('span',class_='ratingCount').text.strip('()')
        if product.find('span',class_='ratingCount') else None
        for product in product_card
    ]
    data = {
        'Brand': product_brand_name,
        'product': product_name_list,
        'Price': product_prices,
        'Rating Score': product_rating_scores,
        'Rating Count': product_rating_count
    }
    df = pd.DataFrame(data)
    return df

def extract_from_page():
    """Extract the data from page"""
    combined_df = pd.DataFrame()
    for i in range(1,1500):
        df = scrape_from_market(i)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    return combined_df

def load_page_csv():
    """ Load the data into csv format"""
    extracted_page = extract_from_page()
    file_path = extracted_page.to_csv('C:/Users/103em/Desktop/Python/Trendyol_Elt_project/trendyol_laptop.csv',index=False)
    return file_path

csv_path = load_page_csv()
print(f"Data has been saved to {csv_path}")
