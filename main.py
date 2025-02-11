import os
import time
import json
import logging
import psycopg2
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

#Utilização do logging para registrar eventos do script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#Carregar variáveis de ambiente do arquivo .env
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

#Configuração do Selenium sem interace grafica
options = Options()
options.add_argument("--headless")
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

#URL do site do TCE-SP para raspagem de dados
URL = "https://www.tce.sp.gov.br/jurisprudencia/"
driver.get(URL)
time.sleep(3)  

#realizando a busca no site
try:
    search_box = driver.find_element(By.XPATH, "/html/body/form/div/div[2]/div[2]/div[1]/div/input")
    search_box.send_keys("fraude em escolas")
    executa_button = driver.find_element(By.XPATH, "/html/body/form/div/div[2]/div[3]/div[9]/div/input[1]")
    executa_button.click()
    time.sleep(5) 
except Exception as e:
    logging.error(f"Erro ao interagir com a página de busca: {e}")
    driver.quit()
    exit()

#Coletar os documentos da tabela de resultados
documentos = []
tbody_xpath = "/html/body/table/tbody"
index = 1  #index para coletar os resultados um por um

while True:
    try:
        row_xpath = f"{tbody_xpath}/tr[{index}]"
        row = driver.find_element(By.XPATH, row_xpath)

        #Coletar dados das colunas
        doc = row.find_element(By.XPATH, "td[1]").text.strip()
        n_processo = row.find_element(By.XPATH, "td[2]").text.strip()
        data_atuacao = row.find_element(By.XPATH, "td[3]").text.strip()
        partes = row.find_element(By.XPATH, "td[4]").text.strip() + " | " + row.find_element(By.XPATH, "td[5]").text.strip()
        materia = row.find_element(By.XPATH, "td[6]").text.strip()

        #capturar link do documento, se existir
        try:
            url = row.find_element(By.XPATH, "td[1]/a").get_attribute("href")
        except:
            url = "N/A"

        documentos.append((doc, n_processo, data_atuacao, partes, materia, url))
        index += 2  #Pular para próxima linha
    except:
        break  #Sai do loop quando não houver mais linhas

driver.quit()

#Estrutura do JSON para salvar os dados
documentos_json = [
    {
        "Doc": doc,
        "N processo": n_processo,
        "Data Atuação": data_atuacao,
        "Partes": partes.split(" | "),
        "Matéria": materia,
        "url": url
    }
    for doc, n_processo, data_atuacao, partes, materia, url in documentos
]

json_filename = "documentos_tce.json"
with open(json_filename, "w", encoding="utf-8") as file:
    json.dump(documentos_json, file, ensure_ascii=False, indent=4)

logging.info(f"Dados extraídos e salvos em {json_filename}.")

#Funções para interação com o banco de dados
def connect_db():
    """Estabelece conexão com o banco de dados PostgreSQL."""
    return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)

def create_table():
    """Cria a tabela de documentos no banco de dados, se ainda não existir."""
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS documentos (
        id SERIAL PRIMARY KEY,
        doc VARCHAR(100),
        n_processo VARCHAR(50),
        data_atuacao DATE,
        partes TEXT,
        materia VARCHAR(100),
        url TEXT
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_data ON documentos (data_atuacao);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_materia ON documentos (materia);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_doc ON documentos (doc);")
    conn.commit()
    cur.close()
    conn.close()

def insert_data():
    """Insere os documentos extraídos no banco de dados."""
    conn = connect_db()
    cur = conn.cursor()
    for doc, n_processo, data_atuacao, partes, materia, url in documentos:
        cur.execute("""
            INSERT INTO documentos (doc, n_processo, data_atuacao, partes, materia, url)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (doc, n_processo, data_atuacao, partes, materia, url))
    conn.commit()
    cur.close()
    conn.close()

#executar as operações no banco de dados
try:
    create_table()
    insert_data()
    logging.info("Dados inseridos no banco de dados com sucesso.")
except Exception as e:
    logging.error(f"Erro ao manipular banco de dados: {e}")
