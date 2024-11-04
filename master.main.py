import pandas as pd
import requests
from requests_kerberos import HTTPKerberosAuth
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
from datetime import date
from pandas import json_normalize
import random
import os

# Definições do proxy
proxy_port = '8080'
proxy_ip = 'rb-proxy-de.bosch.com'

class HTTPAdapterWithProxyKerberosAuth(requests.adapters.HTTPAdapter):
    def proxy_headers(self, proxy):
        headers = {}
        auth = HTTPKerberosAuth()
        parsed_url = urlparse(proxy)
        negotiate_details = auth.generate_request_header(None, parsed_url.hostname, is_preemptive=True)
        headers['Proxy-Authorization'] = negotiate_details
        return headers

# Função para fazer a requisição com retry e Kerberos Auth
def fetch_with_retry(url, retries=3, backoff_factor=2.0):
    session = requests.Session()
    session.proxies = {
        "http": f'http://{proxy_ip}:{proxy_port}',
        "https": f'http://{proxy_ip}:{proxy_port}'
    }
    session.mount('http://', HTTPAdapterWithProxyKerberosAuth())
    session.mount('https://', HTTPAdapterWithProxyKerberosAuth())

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

    for i in range(retries):
        try:
            response = session.get(url, headers=headers)
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                wait_time = backoff_factor * (2 ** i)
                print(f"Erro 429. Tentando novamente em {wait_time} segundos...")
                time.sleep(wait_time)
            elif response.status_code == 403:
                wait_time = backoff_factor * (10 ** i)
                print(f"Erro 403. Tentando novamente em {wait_time} segundos...")
                time.sleep(wait_time)
            else:
                print(f"Erro ao acessar a página: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição: {e}")
            return None
    return None

# Função para capturar dados de uma URL
def get_product_info(url):
    response = fetch_with_retry(url)
    if response is None:
        return None, None, None

    soup = BeautifulSoup(response.text, 'html.parser')

    try:
        price_regular = soup.find('li', class_='jsx-329924046 prices-0')
        if price_regular:
            price_regular = float(price_regular.text.strip().replace('$', '').replace(' / Unidad', '').replace('.', '').replace(',', '.'))
            print(f"Preço regular encontrado: {price_regular}")
        else:
            price_regular = None
            print("Preço regular não encontrado.")
    except:
        price_regular = None
        print("Erro ao extrair preço regular.")

    try:
        price_discount = soup.find('div', class_='jsx-2101313034 cmr-icon-container')
        if price_discount:
            price_discount = price_discount.text.strip().split('/ Unidad')[0].strip()
            price_discount = float(price_discount.replace('$', '').replace(' / Unidad', '').replace('.', '').replace(',', '.'))
            print(f"Preço com desconto encontrado: {price_discount}")
        else:
            price_discount = None
            print("Preço com desconto não encontrado.")
    except:
        price_discount = None
        print("Erro ao extrair preço com desconto.")

    price = price_discount if price_discount and (price_regular is None or price_discount < price_regular) else price_regular
    print(f"Preço final utilizado: {price}")

    try:
        seller_tag = soup.find_all('div', id="seller-tooltip-text")
        seller = seller_tag[0].find('b').get_text() if seller_tag else None
    except:
        seller = None

    try:
        image_tag = soup.find_all('img', class_='jsx-2487856160')
        image = image_tag[0]['src'] if image_tag else None
    except:
        image = None

    return price, seller, image

# Função para coletar informações do Mercado Libre
def fetch_mercado_libre():
    tabela = pd.read_excel('S:/PT/ac-la/AC_MKB/7. TP ON/E-dealers/01_EspejoDePrecios/v1/Meli/EANList.xlsx')
    urlModel = 'https://api.mercadolibre.com/sites/MLC/search?q='
    headers = {'Authorization': 'Bearer eNC2j5Yh5IEofjqu0YujyJmgiEw36MSp'}
    df = pd.DataFrame()

    for i in tqdm(tabela.index):
        eanSTR = str(tabela.loc[i, "EAN"]).replace('.0','')
        url = urlModel + eanSTR

        session = requests.Session()
        session.proxies = {"http": f'{proxy_ip}:{proxy_port}', "https": f'{proxy_ip}:{proxy_port}'}
        session.mount('http://', HTTPAdapterWithProxyKerberosAuth())
        session.mount('https://', HTTPAdapterWithProxyKerberosAuth())
        response = session.get(url, headers=headers)

        if response.status_code == 200:
            json_data = response.json()
            if 'results' in json_data and len(json_data['results']) > 0:
                df_temp = pd.DataFrame(json_data['results'])
                df_temp['query'] = eanSTR
                now = date.today()
                df_temp['dateSearch'] = now
                df_temp['source'] = 'Meli'
                for col in ['thumbnail', 'permalink', 'price', 'seller.id']:
                    if col not in df_temp.columns:
                        df_temp[col] = None
                df_temp = df_temp.rename(columns={'seller.id': 'seller'})
                df_temp = df_temp[['query', 'dateSearch', 'thumbnail', 'permalink', 'price', 'seller', 'source']]
                for _, row in df_temp.iterrows():
                    print(f"Preço encontrado no Mercado Libre para EAN {row['query']}: {row['price']}")
                df = pd.concat([df, df_temp], ignore_index=True, sort=False)

        time.sleep(0.5)

    df.to_csv(f'S:/PT/ac-la/AC_MKB/7. TP ON/E-dealers/01_EspejoDePrecios/v1/Meli/Search_results/Meli/{date.today()}_MeLiSearchTeste.csv', index=False)
    return df

# Função para coletar informações da Sodimac
def fetch_sodimac():
    df = pd.read_excel('S:/PT/ac-la/AC_MKB/7. TP ON/E-dealers/01_EspejoDePrecios/v1/Sodimac/Base_para_Busca.xlsx')
    results = []

    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        url = row['Links']
        if not isinstance(url, str) or not url.strip():
            continue

        price, seller, image = get_product_info(url)
        print(f"Preço encontrado na Sodimac para SKU {row['SKU']}: {price}")
        sku = row['SKU']
        codSodimac = row['CodSodimac']
        today = date.today()
        results.append({
            'query': sku,
            'dateSearch': today,
            'thumbnail': image,
            'permalink': url,
            'price': price,
            'seller': seller,
            'source': 'Sodimac'
        })

    df_results = pd.DataFrame(results)
    df_results.to_csv(f'S:/PT/ac-la/AC_MKB/7. TP ON/E-dealers/01_EspejoDePrecios/v1/Sodimac/Searching/resultadosBuscaSodimacChile{date.today()}.csv', index=False)
    return df_results

# Função principal para consolidar dados de ambas as fontes
def main():
    df_mercado_libre = fetch_mercado_libre().reset_index(drop=True)
    df_sodimac = fetch_sodimac().reset_index(drop=True)

    # Garantir que ambos os DataFrames tenham as mesmas colunas e remover duplicatas de colunas
    common_columns = ['query', 'dateSearch', 'price', 'thumbnail', 'permalink', 'seller', 'source']
    df_mercado_libre = df_mercado_libre.loc[:, ~df_mercado_libre.columns.duplicated()].reindex(columns=common_columns)
    df_sodimac = df_sodimac.loc[:, ~df_sodimac.columns.duplicated()].reindex(columns=common_columns)

    # Consolidando resultados em um único DataFrame
    df_master = pd.concat([df_mercado_libre, df_sodimac], ignore_index=True, sort=False)

    # Carregar o arquivo master existente se houver, para consolidar
    master_file_path = 'S:/PT/ac-la/AC_MKB/7. TP ON/E-dealers/01_EspejoDePrecios/v1/Master/MasterPrice.csv'
    try:
        df_master_existing = pd.read_csv(master_file_path).reset_index(drop=True)
        df_master = pd.concat([df_master_existing, df_master], ignore_index=True, sort=False)
    except FileNotFoundError:
        print("Arquivo MasterPrice.csv não encontrado, criando um novo.")

    # Salvar backup do master antes de sobrescrever
    backup_dir = 'S:/PT/ac-la/AC_MKB/7. TP ON/E-dealers/01_EspejoDePrecios/v1/Master/Backup'
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    backup_path = f'{backup_dir}/MasterPrice_Backup_{date.today()}.csv'
    df_master.to_csv(backup_path, index=False)
    print(f"Backup do arquivo master salvo em {backup_path}")

    # Salvar o arquivo master atualizado
    df_master.to_csv(master_file_path, index=False)
    print(f"Consolidação completa e salva em MasterPrice.csv")

if __name__ == "__main__":
    main()
