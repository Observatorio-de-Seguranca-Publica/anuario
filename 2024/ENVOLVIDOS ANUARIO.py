import pandas as pd
import geopandas as gpd
import numpy as np
from impala.dbapi import connect
import pyproj
import requests
import tempfile
import os

# Função para ler o arquivo de credenciais
def get_credentials(file_path):
    credentials = {}
    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            credentials[key] = value
    return credentials

# Função para conectar ao banco de dados
def get_conn_and_cursor(db='db_bisp_reds_reporting', credentials_file='C:\\Users\\x20081782\\OneDrive - CAMG\\Área de Trabalho\\Paineis\\Credenciamento Python.txt'):
    credentials = get_credentials(credentials_file)
    conn = connect(host='10.100.62.20', port=21051, use_ssl=True, auth_mechanism="PLAIN",
                   user=credentials['username'], password=credentials['password'], database=db)
    cursor = conn.cursor()
    return conn, cursor

# Função para executar query e retornar dataframe
def executa_query_retorna_df(query, db='db_bisp_reds_reporting'):
    conn, cursor = get_conn_and_cursor(db)  
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [c[0] for c in cursor.description]
    df = pd.DataFrame(results, columns=columns)
    conn.close()
    return df

# Função para listar tabelas no banco de dados
def tabelas(filtro='', db='db_bisp_reds_reporting'):
    conn, cursor = get_conn_and_cursor(db)
    cursor.execute('SHOW TABLES')
    tabelas_nomes = cursor.fetchall()    
    conn.close()
    tabelas_filtradas = [tupla_tabela[0] for tupla_tabela in tabelas_nomes if filtro in tupla_tabela[0]]
    return tabelas_filtradas

# Função para listar bancos de dados
def bancos_de_dados():
    conn, cursor = get_conn_and_cursor()
    try:
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        accessible_databases = []
        for db in databases:
            try:
                cursor.execute(f"USE {db[0]}")
                accessible_databases.append(db[0])
            except:
                pass
        return accessible_databases
    finally:
        cursor.close()
        conn.close()

# Consulta ao banco (script do dbeaver: no exemplo abaixo há um join entre a tabela de ocorrências e envolvidos)
try:
    query = """
    SELECT oco.numero_ocorrencia,
                      oco.qtd_ocorrencia,
                      oco.natureza_descricao,
                      oco.natureza_consumado,
                      CAST (oco.data_hora_fato as date) as data_fato,
                      YEAR (oco.data_hora_fato) as ano_fato,
                      MONTH (oco.data_hora_fato) as mes_numerico_fato,
                      SUBSTRING(CAST(oco.data_hora_fato AS STRING), 12, 8) AS horario_fato,
                      oco.motivo_presumido_descricao_longa,
                      oco.instrumento_utilizado_descricao_longa,
                      oco.local_imediato_longa,
                      oco.complemento_natureza_descricao,
                      oco.tipo_logradouro_descricao,
                      oco.descricao_endereco,
                      oco.nome_bairro,
                      oco.nome_municipio,
                      oco.codigo_municipio,
                      oco.ocorrencia_uf,
                      oco.unidade_responsavel_registro_nome,
                      oco.numero_latitude,
                      oco.numero_longitude,
                      oco.tipo_local_descricao,
                      env.numero_envolvido,
                      env.valor_idade_aparente,
                      env.envolvimento_descricao_longa,
                      env.relacao_vitima_autor_descricao,
                      env.escolaridade_descricao_longa,
                      env.natureza_ocorrencia_codigo,
                      env.natureza_ocorrencia_descricao,
                      env.qtd_envolvido,
                      env.condicao_fisica_descricao,
                      env.ind_consumado,
                      env.cor_pele_descricao,
                      env.descricao_ocupacao_profissional,
                      env.estado_civil_descricao_longa,
                      env.codigo_sexo,
                      env.identidade_genero_descricao
               FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
               LEFT JOIN db_bisp_reds_reporting.tb_envolvido_ocorrencia AS env
                    ON oco.numero_ocorrencia = env.numero_ocorrencia
               WHERE oco.data_hora_fato >= '2020-01-01 00:00:00.000'
               AND oco.data_hora_fato < '2025-01-01 00:00:00.000'
               AND oco.data_hora_inclusao <= '2025-03-17 23:59:59.000'
               AND oco.ocorrencia_uf = 'MG'
               AND oco.ind_estado IN ('F', 'R')
               AND oco.nome_tipo_relatorio IN ('POLICIAL','REFAP')
               AND env.envolvimento_codigo IN ('1300','1301','1302','1303','1304','1305','1399', '0100', '1100', '0200')
               AND env.natureza_ocorrencia_codigo IN ('C01157')
               AND env.ind_consumado = 'S'

"""
    
    df = executa_query_retorna_df(query, db='db_bisp_reds_reporting')


except Exception as e:
    print(f"Erro ao consultar a tabela 'tb_ocorrencia': {e}")

# URL do arquivo no GitHub
url = 'https://github.com/barbaraoliveira-hub/SAD_IBGE96/raw/main/SAD96_003.GSB'

# Baixar o arquivo temporariamente
response = requests.get(url)
response.raise_for_status()  # Garantir que o download foi bem-sucedido

# Criar um arquivo temporário para o arquivo baixado
with tempfile.NamedTemporaryFile(delete=False, suffix='.GSB') as temp_file:
    temp_file.write(response.content)
    temp_file_path = temp_file.name

# Define o pipeline de transformação
transformer = pyproj.Transformer.from_pipeline(
    f"+proj=pipeline +step +proj=axisswap +order=2,1 "
    "+step +proj=unitconvert +xy_in=deg +xy_out=rad "
    f"+step +proj=hgridshift +grids={temp_file_path} "
    "+step +proj=unitconvert +xy_in=rad +xy_out=deg "
    "+step +proj=axisswap +order=2,1"
)

# Função para transformar coordenadas
def transformar_coordenadas(lat, lon):
    if pd.isna(lat) or pd.isna(lon):
        return pd.Series(["", ""], index=['Latitude SIRGAS', 'Longitude SIRGAS'])
    lat_sirgas, lon_sirgas = transformer.transform(lat, lon)
    return pd.Series([lat_sirgas, lon_sirgas], index=['Latitude SIRGAS', 'Longitude SIRGAS'])

# Criar novas colunas com as coordenadas transformadas no DataFrame retornado pela consulta SQL
df[['Latitude SIRGAS', 'Longitude SIRGAS']] = df.apply(
    lambda row: transformar_coordenadas(row['numero_latitude'], row['numero_longitude']),
    axis=1
)
# Função para categorizar a faixa etária
def faixa_etaria(valor_idade_aparente):
    if pd.isnull(valor_idade_aparente) or valor_idade_aparente < 0:
        return 'NÃO INFORMADO'
    elif valor_idade_aparente <= 11:
        return '0 a 11 anos'
    elif valor_idade_aparente <= 17:
        return '12 a 17 anos'
    elif valor_idade_aparente <= 24:
        return '18 a 24 anos'
    elif valor_idade_aparente <= 29:
        return '25 a 29 anos'
    elif valor_idade_aparente <= 34:
        return '30 a 34 anos'
    elif valor_idade_aparente <= 64:
        return '35 a 64 anos'
    else:
        return '65 anos ou mais'
    
    
# Criando nova coluna sem substituir
df['Faixa Etária'] = df['valor_idade_aparente'].apply(faixa_etaria)

# Exibe as primeiras linhas do DataFrame após adicionar as novas colunas
df.head()

# Exporta a base no computador no modelo desejado 
df.to_excel("C:\\Users\\x20081782\\Downloads\\Envolvidos_roubo_2020 a 2024.xlsx", index=False)
