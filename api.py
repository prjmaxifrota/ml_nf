import requests, base64
import re, json, time, os
from dotenv import load_dotenv

load_dotenv()

def get_token(client_id, client_secret):
    """
    Obtém o token de autenticação via OAuth2.
    """
    url = "https://api.serpro.gov.br/token"
    payload = {
        'grant_type': 'client_credentials'
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.post(url, data=payload, headers=headers, auth=(client_id, client_secret))

    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        raise Exception(f"Erro ao obter token: {response.status_code} - {response.text}")

def get_token_with_basic_auth():
    """
    Obtém o token de autenticação via OAuth2 usando Authorization Basic.
    """
    url = "https://gateway.apiserpro.serpro.gov.br/token"
    consumer_key = os.getenv('SERPRO_CONSUMER_KEY')
    consumer_secret = os.getenv('SERPRO_CONSUMER_SECRET')

    if not consumer_key or not consumer_secret:
        raise Exception("SERPRO_CONSUMER_KEY ou CONSUMER_SECRET não definidos no arquivo .env")

    # Concatenar chave e segredo e codificar em base64
    credentials = f"{consumer_key}:{consumer_secret}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    headers = {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    payload = {
        'grant_type': 'client_credentials'
    }

    response = requests.post(url, headers=headers, data=payload)

    if response.status_code == 200:
        token_data = response.json()
        token_data['last_time_request'] = time.time()
        with open('token.json', 'w') as file:
            json.dump(token_data, file)
        return token_data
    else:
        raise Exception(f"Erro ao obter token: {response.status_code} - {response.text}")

def get_valid_token():
    """
    Retorna um token válido. Se expirado ou não existir, obtém um novo token.
    """
    token_file = 'token.json'

    if os.path.exists(token_file):
        try:
            with open(token_file, 'r') as file:
                token_data = json.load(file)
                expires_in = token_data.get('expires_in', 0)
                last_time_request = token_data.get('last_time_request', 0)

                if time.time() - last_time_request < expires_in:
                    return token_data['access_token']
                else:
                    print("Token expirado. Obtendo um novo token...")
        except (json.JSONDecodeError, KeyError):
            print("Erro ao ler o arquivo de token. Obtendo um novo token...")

    # Se o token não existe ou está inválido, obter um novo
    token_data = get_token_with_basic_auth()
    return token_data['access_token']

def consultar_nfs_por_cnpj(tokenAutorizacao, tokenApiClient, cnpj):
    """
    Consulta várias NFes associadas a um CNPJ na API do Serpro.
    """
    
    url = os.getenv('SERPRO_API_NF_POR_CNPJ')
    if url is None:
        raise Exception('URL da API de NFe [SERPRO_API_NF_POR_CNPJ] não informado em consultar_nfs_por_cnpj()')

    url.replace('{tokenAutorizacao}', tokenAutorizacao)
    url.replace('{cnpj}', cnpj)

    headers = {
        'Authorization': f'Bearer {tokenApiClient}',
        'Content-Type': 'application/json'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erro ao consultar NFes por CNPJ: {response.status_code} - {response.text}")

def remove_non_numeric_chars(input_string):
    """
    Remove caracteres não numéricos de uma string.

    Args:
        input_string (str): A string de entrada.

    Returns:
        str: A string contendo apenas caracteres numéricos.
    """
    return re.sub(r'\D', '', input_string)

if __name__ == "__main__":
    
    
    cliente_nf = [
        { 
            'tokenAutorizacao': '?',
            'cnpj': '42194191000110'
        }
    ]
    
    autorizacao = cliente_nf[0]['tokenAutorizacao']
    cnpj = cliente_nf[0]['cnpj']
    
    # Substitua pelas credenciais da API do Serpro
    consumer_key = os.getenv('CONSUMER_KEY')
    consumer_secret = os.getenv('CONSUMER_SECRET')

    try:
        print("Obtendo token válido...")
        token = get_valid_token()
        print(f"Token obtido")

        # Remover caracteres não numéricos do CNPJ
        cnpj_clean = remove_non_numeric_chars(cnpj)
        print(f"CNPJ após limpeza: {cnpj_clean}")

        print("Consultando NFes por CNPJ...")
        
        # Criar tabela de associação (ou alterar tabela de clientes no sistema) tokenAutorizacao do CNPJ
        resultado = consultar_nfs_por_cnpj(autorizacao, token, cnpj_clean, '2024-10-01', '2024-10-31')

        print("Resultado da consulta:")
        print(resultado)
    except Exception as e:
        print(f"Erro: {e}")
