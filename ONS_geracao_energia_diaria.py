"""
Script de Geração de Energia Diária do ONS
Autor: João Miguel de Abreu Constâncio
Descrição: Este script busca dados de geração de energia de diferentes subsistemas do Operador Nacional do Sistema Elétrico (ONS) utilizando a API oficial e salva os dados em um arquivo CSV.
Ultima alteração: 20/02/2023
Versão: 1.0
"""


import requests
import pandas as pd
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# URL da API e token de autenticação
base_url = "https://integra.ons.org.br/api/energiaagora/Get/"
api_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy5taWNyb3NvZnQuY29tL0xpdmVJRC9GZWRlcmF0aW9uLzIwMDgvMDUvSW1tdXRhYmxlSUQiOiJTLTEtOS0xMDg5MDU5NTAzLTc3NTc4ODI1Mi04ODM4MjQ5NyIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2F1dGhvcml6YXRpb25kZWNpc2lvbiI6IkFQSUdBVEVXQVkiLCJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9hdXRoZW50aWNhdGlvbiI6IlBPUCIsImlhdCI6MTYzNzMzMzI0MiwiaHR0cDovL3NjaGVtYXMuZXhnbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL3ByaW1hcnlzaWQiOiIiLCJodHRwOi8vc2NoZW1hcy5taWNyb3NvZnQuY29tL3dzLzIwMDgvMDYvaWRlbnRpdHkvY2xhaW1zL3ByaW1hcnlzaWQiOiJTLTEtOS0xMDg5MDU5NTAzLTc3NTc4ODI1Mi04ODM4MjQ5NyIsImh0dHA6Ly9zY2hlbWFzLmV4Z25sc29hcC5vcmcvd3MvMjAwNS8wNS9pZGVudGl0eS9jbGFpbXMvZW1haWwiOiJyZXNlYXJjaEAyd2VuZXJnaWEuY29tLmJyIiwidWlkIjoiNDE5MzkiLCJodHRwOi8vc2NoZW1hcy5leG5sc29hcC5vcmcvd3MvMjAwNS8wNS9pZGVudGl0eS9jbGFpbXMvZW1haWwiOiJyZXNlYXJjaEAyd2VuZXJnaWEuY29tLmJyIiwibmJmIjoxNjM3MzMzNDQxLCJleHAiOjE2MzczMzcwNDEsImlzcyI6Imh0dHBzOi8vcG9wcy5vbnMub3JnLmJyL29ucy5wb3AuZmVkZXJhdGlvbi8iLCJhdWQiOiJBUklHQVRFV0FZIn0.lOsguvHtS4SQbobMEADo2dMlKGAHtc_sloEXQg8zwy8rJCXIbC5quZUm3HLbmwb9vwRXaB8eS9LzYVs8d1g_wKhSzC9MTDET9zgyYlppiH5-0xhyJsm3A3NSItWp2ub6qDy-ORXtewVhulVvas6NZrYdu-yqvDKzyrejUJN60IxeN12zDaYn2PAcBiovEz56GMt4ZCKXsVsA8BguHJIXFnag-uw5qNHCYA7xlBbADh3NTHLF7lJmRNMKBTA8dEtIiDjDn2ZRDzu4ZS43pIjwXsbbkFea8Mw3uL5ztw69vJPV2BXBhHjoyWxslBWIkiMtbGnEvHWNHjRJwAMHwDY5XA"

headers = {
    "Authorization": f"Bearer {api_token}"
}

def call_api(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao buscar dados: {response.status_code} - {response.text}")
        return None

def main():
    subsistemas = {
        "Geracao_SIN_Eolica_json": "SIN_Eolica",
        "Geracao_SIN_Hidraulica_json": "SIN_Hidraulica",
        "Geracao_SIN_Nuclear_json": "SIN_Nuclear",
        "Geracao_SIN_Solar_json": "SIN_Solar",
        "Geracao_SIN_Termica_json": "SIN_Termica",
        "Geracao_Norte_Eolica_json": "Norte_Eolica",
        "Geracao_Norte_Hidraulica_json": "Norte_Hidraulica",
        "Geracao_Norte_Solar_json": "Norte_Solar",
        "Geracao_Norte_Termica_json": "Norte_Termica",
        "Geracao_Nordeste_Eolica_json": "Nordeste_Eolica",
        "Geracao_Nordeste_Hidraulica_json": "Nordeste_Hidraulica",
        "Geracao_Nordeste_Solar_json": "Nordeste_Solar",
        "Geracao_Nordeste_Termica_json": "Nordeste_Termica",
        "Geracao_SudesteECentroOeste_Eolica_json": "SudesteECentroOeste_Eolica",
        "Geracao_SudesteECentroOeste_Hidraulica_json": "SudesteECentroOeste_Hidraulica",
        "Geracao_SudesteECentroOeste_Nuclear_json": "SudesteECentroOeste_Nuclear",
        "Geracao_SudesteECentroOeste_Solar_json": "SudesteECentroOeste_Solar",
        "Geracao_SudesteECentroOeste_Termica_json": "SudesteECentroOeste_Termica",
        "Geracao_Sul_Eolica_json": "Sul_Eolica",
        "Geracao_Sul_Hidraulica_json": "Sul_Hidraulica",
        "Geracao_Sul_Solar_json": "Sul_Solar",
        "Geracao_Sul_Termica_json": "Sul_Termica"
    }

    # Obtém data atual para criar as pastas
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m_%B")
    
    # define o caminho das pastas
    root_dir = os.path.join(os.getcwd(), "ONS geração de energia diaria") #cria o caminho para o diretório raiz onde todas as pastas serão armazenadas
    #cria o caminho para o diretório do ano, combinando o caminho do diretório raiz
    year_dir = os.path.join(root_dir, year)
    #cria o caminho para o diretório do mês, combinando o caminho do diretório do ano
    month_dir = os.path.join(year_dir, month)
    
    # cria as pastas se não existirem
    os.makedirs(month_dir, exist_ok=True)
    
    data_frames = []

    #Define o número de threads com base no número de CPUs disponíveis
    num_cpus = os.cpu_count()
    #multiplica o número de CPUs por 2 para igualar ao número de threads disponíveis no sistema
    max_workers = num_cpus * 2 
    # Cria um ThreadPoolExecutor para gerenciar cada chamada da API para os treads disponíveis
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(call_api, f"{base_url}{subsistema}"): col_name for subsistema, col_name in subsistemas.items()}
        
        for future in as_completed(future_to_url):
            col_name = future_to_url[future]
            try:
                # Obter o resultado da chamada de API
                dados = future.result()
                if dados:
                    # Se dados foram obtidos, criar um dataframe e renomear a coluna
                    df = pd.DataFrame(dados)
                    df.rename(columns={df.columns[0]: col_name}, inplace=True)
                    data_frames.append(df)
                    print(f"Dados obtidos para {col_name}")
                else:
                    print(f"Não foi possível obter dados para {col_name}.")
            except Exception as e:
                print(f"Erro ao processar {col_name}: {e}")

    if data_frames:
        result = pd.concat(data_frames, axis=1)
        
        # adiciona data e hora no nome do arquivo .CSV
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        csv_file = os.path.join(month_dir, f"geracao_energia_{timestamp}.csv")
        
        result.to_csv(csv_file, index=False, sep=';', decimal=',')
        print(f"Todos os dados salvos com sucesso em {csv_file}")
    else:
        print("Não foram obtidos dados de nenhum subsistema.")

if __name__ == "__main__":
    main()
