# Geracao_Energia_ONS_API
Este script é responsável por buscar dados de geração de energia de diferentes subsistemas do Operador Nacional do Sistema Elétrico (ONS) utilizando a API oficial. Ele salva esses dados em um arquivo CSV. O script foi otimizado para ser executado de forma paralela, utilizando threads para melhorar o desempenho.

# Instruções para o Script de Geração de Energia Diária do ONS

## Descrição do Código

Este script é responsável por buscar dados de geração de energia de diferentes subsistemas do Operador Nacional do Sistema Elétrico (ONS) utilizando a API oficial. Ele salva esses dados em um arquivo CSV. O script foi otimizado para ser executado de forma paralela, utilizando threads para melhorar o desempenho.

## O que o Código Faz

1. Define a URL base da API e o token de autenticação.
2. Define uma função `call_api` para fazer chamadas à API e obter os dados.
3. Obtém a data atual e cria pastas organizadas por ano e mês para salvar os dados.
4. Utiliza um `ThreadPoolExecutor` para fazer chamadas de API em paralelo para diferentes subsistemas.
5. Coleta e organiza os dados retornados das chamadas de API.
6. Salva os dados em um arquivo CSV com um timestamp no nome.

## Como Rodar o Código

### Pré-requisitos

- Python 3.7 ou superior
- Bibliotecas Python:
  - `requests`
  - `pandas`

### Passos para Rodar o Código

1. **Instale as bibliotecas necessárias:**

   Abra o terminal e execute:
   ```bash
   pip install requests pandas
