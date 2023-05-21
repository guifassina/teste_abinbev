import requests
import json
from os.path import join
from pathlib import Path

#URL da API
url = "https://api.openbrewerydb.org/v1/breweries"

# Variaveis
#file_path = f'/home/guilherme/Documents/datapipeline/list_breweries.json'
file_path = join(str(Path("~/Documents").expanduser()),"teste_ABInBev/datalake/bronze/list_breweries.json")


# Fazer request da API
response = requests.get(url)

# Testa se response da API foi OK
if response.status_code == 200:
    # Converter response em JSON
    dados = response.json()
    # Salva os dados em JSON
    with open(file_path, "w") as file:
        json.dump(dados, file)
    print("Dados salvos em JSON")
else:
    print("Erro ao acessar API: ", response.status_code)