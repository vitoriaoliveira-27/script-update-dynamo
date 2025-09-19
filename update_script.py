import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

load_dotenv()
# --- CONFIGURAÇÕES ---
# Altere estas variáveis de acordo com o seu ambiente
NOME_DA_TABELA = "docspygrown.parameters"  # Ex: "transactions"
NOME_ARQUIVO_IDS = "chave_particao.txt"
NOME_CAMPO_CHAVE = "idIssuer"                 # Nome da chave primária da sua tabela
NOME_CAMPO_ALVO = "typification"
NOVO_VALOR = False

# --- INÍCIO DO SCRIPT ---

def atualizar_registros():
    """
    Função principal que lê os IDs do arquivo e atualiza os registros no DynamoDB.
    """
    # Inicializa o cliente do DynamoDB usando credenciais do .env
    try:
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "sa-east-1")
        aws_session_token = os.getenv("AWS_SESSION_TOKEN")
        dynamodb = boto3.resource(
            'dynamodb',
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )
        tabela = dynamodb.Table(NOME_DA_TABELA)
    except Exception as e:
        print(f"Erro ao conectar com o AWS DynamoDB: {e}")
        return

    # Tenta ler os issuerId's do arquivo de texto
    try:
        with open(NOME_ARQUIVO_IDS, 'r') as f:
            # .strip() remove espaços em branco e quebras de linha
            issuer_ids = [line.strip() for line in f if line.strip()]
        if not issuer_ids:
            print(f"Arquivo '{NOME_ARQUIVO_IDS}' está vazio ou não foi encontrado.")
            return
    except FileNotFoundError:
        print(f"Erro: Arquivo '{NOME_ARQUIVO_IDS}' não encontrado no mesmo diretório do script.")
        return

    print(f"Total de {len(issuer_ids)} IDs encontrados para processar.")
    
    sucessos = 0
    falhas = 0

    # Itera sobre cada ID e tenta atualizar o registro correspondente
    for issuer_id in issuer_ids:
        print(f"Processando ID: {issuer_id}...")
        try:
            # O método update_item é usado para modificar um item existente
            response = tabela.update_item(
                # Key: Identifica o item a ser atualizado
                Key={
                    NOME_CAMPO_CHAVE: int(issuer_id)
                },
                # UpdateExpression: Define a ação de atualização.
                # 'SET #campo = :valor' é a sintaxe para definir o valor de um atributo.
                UpdateExpression="SET #campo_alvo = :novo_valor",
                
                # ConditionExpression: (OPCIONAL, MAS RECOMENDADO)
                # Garante que a atualização só ocorra se o campo já for 'true'.
                # Isso evita escritas desnecessárias e possíveis erros.
                ConditionExpression="#campo_alvo = :valor_antigo",
                
                # ExpressionAttributeNames: Mapeia os placeholders na expressão para nomes reais de atributos.
                # Necessário caso o nome do atributo seja uma palavra reservada do DynamoDB.
                ExpressionAttributeNames={
                    '#campo_alvo': NOME_CAMPO_ALVO
                },
                # ExpressionAttributeValues: Define os valores para os placeholders na expressão.
                ExpressionAttributeValues={
                    ':novo_valor': NOVO_VALOR,  # O novo valor que queremos definir (false)
                    ':valor_antigo': True   # O valor que esperamos encontrar (true)
                },
                # ReturnValues: Especifica quais valores do item devem ser retornados após a atualização.
                ReturnValues="UPDATED_NEW"
            )
            print(f"  -> Sucesso! '{NOME_CAMPO_ALVO}' alterado para 'False'. Resposta: {response.get('Attributes')}")
            sucessos += 1

        except ClientError as e:
            # Trata erros específicos da AWS, como a falha na condição
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                print(f"  -> Aviso: O item com ID '{issuer_id}' não foi atualizado pois o campo '{NOME_CAMPO_ALVO}' não era 'true' ou não existe.")
                falhas += 1
            elif e.response['Error']['Code'] == "ResourceNotFoundException":
                 print(f"  -> Erro: O item com ID '{issuer_id}' não foi encontrado na tabela '{NOME_DA_TABELA}'.")
                 falhas += 1
            else:
                print(f"  -> Erro desconhecido da AWS ao atualizar '{issuer_id}': {e.response['Error']['Message']}")
                falhas += 1
        except Exception as e:
            print(f"  -> Erro geral ao processar o ID '{issuer_id}': {e}")
            falhas += 1

    print("\n--- RESUMO DA EXECUÇÃO ---")
    print(f"Total de atualizações com sucesso: {sucessos}")
    print(f"Total de falhas ou itens não alterados: {falhas}")
    print("----------------------------\n")


if __name__ == "__main__":
    atualizar_registros()