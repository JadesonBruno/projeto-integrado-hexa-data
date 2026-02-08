# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 Configuração e Utilitários Centrais
# MAGIC
# MAGIC **Objetivo**: Módulo compartilhado com configurações, schemas e funções auxiliares
# MAGIC
# MAGIC **Uso**: `%run ./config_utils`
# MAGIC
# MAGIC **Características**:
# MAGIC - Sem Unity Catalog (usa databases tradicionais)
# MAGIC - Suporte a wildcards para nomenclatura variável
# MAGIC - Descompactação automática de ZIPs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime
import json
import zipfile
import os
from pathlib import Path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configurações Globais

# COMMAND ----------

class Config:
    """Configurações centralizadas do pipeline"""
    
    # Databases (sem Unity Catalog)
    DATABASE_BRONZE = "bronze"
    DATABASE_SILVER = "silver"
    DATABASE_GOLD = "gold"
    DATABASE_CONTROL = "control"
    
    # Paths DBFS
    BRONZE_PATH = "/mnt/datalake/bronze"
    SILVER_PATH = "/mnt/datalake/silver"
    GOLD_PATH = "/mnt/datalake/gold"
    
    # Landing Zone
    LANDING_ZONE = "/mnt/landing"
    LANDING_RAW = "/mnt/landing/raw"           # ZIPs chegam aqui
    LANDING_EXTRACTED = "/mnt/landing/extracted"  # CSVs extraídos
    LANDING_PROCESSED = "/mnt/landing/processed"  # ZIPs processados
    
    # Checkpoint
    CHECKPOINT_BASE = "/mnt/checkpoints"
    
    # Processamento
    MAX_FILES_PER_TRIGGER = 100
    AUTO_UNZIP = True  # Habilitar descompactação automática
    
    # Wildcards para nomenclatura variável de arquivos
    FILE_PATTERNS = {
        # Balança Comercial - aceita múltiplos formatos de nome
        "dim_pais": ["PAIS*.csv", "Pais*.csv", "pais*.csv"],
        "dim_via": ["VIA*.csv", "Via*.csv", "via*.csv"],
        "dim_ufr": ["URF*.csv", "Urf*.csv", "urf*.csv"],
        "dim_ncm_unidade": ["NCM_UNIDADE*.csv", "ncm_unidade*.csv"],
        "dim_uf": ["UF*.csv", "Uf*.csv", "uf*.csv"],
        "dim_ncm": ["NCM*.csv", "Ncm*.csv", "ncm*.csv"],
        "dim_uf_mun": ["UF_MUN*.csv", "uf_mun*.csv", "MUNICIPIO*.csv"],
        "fct_exp": ["EXP*.csv", "EXPORTACAO*.csv", "exp*.csv", "exportacao*.csv"],
        "fct_imp": ["IMP*.csv", "IMPORTACAO*.csv", "imp*.csv", "importacao*.csv"],
        "fct_exp_mun": ["EXP*MUN*.csv", "exp*mun*.csv"],
        "fct_imp_mun": ["IMP*MUN*.csv", "imp*mun*.csv"],
        
        # CNPJ - aceita múltiplos formatos
        "cnaes": ["*.CNAECSV", "*CNAE*.csv", "*cnae*.csv"],
        "motivos": ["*.MOTICSV", "*MOTI*.csv", "*motivo*.csv"],
        "municipios": ["*.MUNICCSV", "*MUNIC*.csv", "*municipio*.csv"],
        "naturezas": ["*.NATJUCSV", "*NATJU*.csv", "*natureza*.csv"],
        "paises": ["*.PAISCSV", "*PAIS*.csv", "*pais*.csv"],
        "qualificacoes": ["*.QUALSCSV", "*QUALS*.csv", "*qualificacao*.csv"],
        "empresas": ["*.EMPRECSV", "*EMPRE*.csv", "*empresa*.csv"],
        "estabelecimentos": ["*.ESTABELE", "*ESTABELE*.csv", "*estabelecimento*.csv"],
        "socios": ["*.SOCIOCSV", "*SOCIO*.csv", "*socio*.csv"],
        "simples": ["*.SIMPLES*.CSV*", "*simples*.csv"]
    }
    
    # Liquid Clustering - Configurações por tabela
    CLUSTER_CONFIGS = {
        # Fatos Balança Comercial
        "fct_exp": ["CO_ANO", "CO_MES", "CO_PAIS"],
        "fct_imp": ["CO_ANO", "CO_MES", "CO_PAIS"],
        "fct_exp_mun": ["CO_ANO", "CO_MES", "SG_UF_MUN"],
        "fct_imp_mun": ["CO_ANO", "CO_MES", "SG_UF_MUN"],
        
        # CNPJ
        "empresas": ["natureza_juridica", "porte_empresa"],
        "estabelecimentos": ["situacao_cadastral", "uf", "municipio"],
        "socios": ["cnpj_basico"],
        
        # Dimensões grandes
        "dim_ncm": ["CO_NCM"],
        "dim_uf_mun": ["SG_UF", "CO_MUN_GEO"]
    }
    
    # CDF - Tabelas que precisam rastrear mudanças
    CDF_ENABLED = [
        "fct_exp", "fct_imp", "fct_exp_mun", "fct_imp_mun",
        "empresas", "estabelecimentos", "socios", "simples"
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schemas

# COMMAND ----------

class Schemas:
    """Schemas de todas as tabelas"""
    
    # === BALANÇA COMERCIAL ===
    
    dim_pais = StructType([
        StructField("CO_PAIS", StringType(), False),
        StructField("CO_PAIS_ISON3", StringType(), True),
        StructField("CO_PAIS_ISOA3", StringType(), True),
        StructField("NO_PAIS", StringType(), True),
        StructField("NO_PAIS_ING", StringType(), True),
        StructField("NO_PAIS_ESP", StringType(), True)
    ])
    
    dim_via = StructType([
        StructField("CO_VIA", StringType(), False),
        StructField("NO_VIA", StringType(), True)
    ])
    
    dim_ufr = StructType([
        StructField("CO_URF", StringType(), False),
        StructField("NO_URF", StringType(), True)
    ])
    
    dim_ncm_unidade = StructType([
        StructField("CO_UNID", StringType(), False),
        StructField("NO_UNID", StringType(), True),
        StructField("SG_UNID", StringType(), True)
    ])
    
    dim_uf = StructType([
        StructField("SG_UF", StringType(), False),
        StructField("CO_UF", StringType(), True),
        StructField("NO_UF", StringType(), True),
        StructField("NO_REGIAO", StringType(), True)
    ])
    
    dim_ncm = StructType([
        StructField("CO_NCM", StringType(), False),
        StructField("CO_UNID", StringType(), True),
        StructField("CO_SH6", StringType(), True),
        StructField("CO_PPE", StringType(), True),
        StructField("CO_PPI", StringType(), True),
        StructField("CO_FAT_AGREG", StringType(), True),
        StructField("CO_CUCI_ITEM", StringType(), True),
        StructField("CO_CGCE_N3", StringType(), True),
        StructField("CO_SIIT", StringType(), True),
        StructField("CO_ISIC_CLASSE", StringType(), True),
        StructField("CO_EXP_SUBSET", StringType(), True),
        StructField("NO_NCM_POR", StringType(), True),
        StructField("NO_NCM_ESP", StringType(), True),
        StructField("NO_NCM_ING", StringType(), True)
    ])
    
    dim_uf_mun = StructType([
        StructField("CO_MUN_GEO", StringType(), False),
        StructField("NO_MUN", StringType(), True),
        StructField("NO_MUN_MIN", StringType(), True),
        StructField("SG_UF", StringType(), True)
    ])
    
    fct_exp = StructType([
        StructField("CO_ANO", StringType(), True),
        StructField("CO_MES", StringType(), True),
        StructField("CO_NCM", StringType(), True),
        StructField("CO_UNID", StringType(), True),
        StructField("CO_PAIS", StringType(), True),
        StructField("SG_UF_NCM", StringType(), True),
        StructField("CO_VIA", StringType(), True),
        StructField("CO_URF", StringType(), True),
        StructField("QT_ESTAT", LongType(), True),
        StructField("KG_LIQUIDO", LongType(), True),
        StructField("VL_FOB", LongType(), True)
    ])
    
    fct_imp = StructType([
        StructField("CO_ANO", StringType(), True),
        StructField("CO_MES", StringType(), True),
        StructField("CO_NCM", StringType(), True),
        StructField("CO_UNID", StringType(), True),
        StructField("CO_PAIS", StringType(), True),
        StructField("SG_UF_NCM", StringType(), True),
        StructField("CO_VIA", StringType(), True),
        StructField("CO_URF", StringType(), True),
        StructField("QT_ESTAT", LongType(), True),
        StructField("KG_LIQUIDO", LongType(), True),
        StructField("VL_FOB", LongType(), True),
        StructField("VL_FRETE", LongType(), True),
        StructField("VL_SEGURO", LongType(), True)
    ])
    
    fct_exp_mun = StructType([
        StructField("CO_ANO", StringType(), True),
        StructField("CO_MES", StringType(), True),
        StructField("SH4", StringType(), True),
        StructField("CO_PAIS", StringType(), True),
        StructField("SG_UF_MUN", StringType(), True),
        StructField("CO_MUN", StringType(), True),
        StructField("KG_LIQUIDO", LongType(), True),
        StructField("VL_FOB", LongType(), True)
    ])
    
    fct_imp_mun = StructType([
        StructField("CO_ANO", StringType(), True),
        StructField("CO_MES", StringType(), True),
        StructField("SH4", StringType(), True),
        StructField("CO_PAIS", StringType(), True),
        StructField("SG_UF_MUN", StringType(), True),
        StructField("CO_MUN", StringType(), True),
        StructField("KG_LIQUIDO", LongType(), True),
        StructField("VL_FOB", LongType(), True)
    ])
    
    # === CNPJ ===
    
    empresas = StructType([
        StructField("cnpj_basico", StringType(), False),
        StructField("razao_social", StringType(), True),
        StructField("natureza_juridica", StringType(), True),
        StructField("qualificacao_resposnavel", StringType(), True),
        StructField("capital_social", StringType(), True),
        StructField("porte_empresa", StringType(), True),
        StructField("ente_federativo", StringType(), True)
    ])
    
    estabelecimentos = StructType([
        StructField("cnpj_basico", StringType(), False),
        StructField("cnpj_ordem", StringType(), False),
        StructField("cnpj_dv", StringType(), False),
        StructField("identificador_matriz_filial", StringType(), True),
        StructField("nome_fantasia", StringType(), True),
        StructField("situacao_cadastral", StringType(), True),
        StructField("data_situacao_cadastral", StringType(), True),
        StructField("motivo_situacao_cadastral", StringType(), True),
        StructField("nome_cidade_exterior", StringType(), True),
        StructField("pais", StringType(), True),
        StructField("data_inicio_atividade", StringType(), True),
        StructField("cnae_fiscal_principal", StringType(), True),
        StructField("cnae_fiscal_secundaria", StringType(), True),
        StructField("tipo_logradouro", StringType(), True),
        StructField("logradouro", StringType(), True),
        StructField("numero", StringType(), True),
        StructField("complemento", StringType(), True),
        StructField("bairro", StringType(), True),
        StructField("cep", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("ddd_1", StringType(), True),
        StructField("telefone_1", StringType(), True),
        StructField("ddd_2", StringType(), True),
        StructField("telefone_2", StringType(), True),
        StructField("ddd_fax", StringType(), True),
        StructField("fax", StringType(), True),
        StructField("correio_eletronico", StringType(), True),
        StructField("situacao_especial", StringType(), True),
        StructField("data_situacao_especial", StringType(), True)
    ])
    
    socios = StructType([
        StructField("cnpj_basico", StringType(), True),
        StructField("identificador_socio", StringType(), True),
        StructField("nome_socio_razao_social", StringType(), True),
        StructField("cnpj_cpf_socio", StringType(), True),
        StructField("qualificacao_socio", StringType(), True),
        StructField("data_entrada_sociedade", StringType(), True),
        StructField("pais", StringType(), True),
        StructField("representante_legal", StringType(), True),
        StructField("nome_representante", StringType(), True),
        StructField("qualificacao_representante", StringType(), True),
        StructField("faixa_etaria", StringType(), True)
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Funções Utilitárias

# COMMAND ----------

def log_execution(pipeline_name, layer, status, records_count=0, error_msg=None, metadata=None):
    """
    Registra execução na tabela de controle
    """
    import uuid
    execution_id = str(uuid.uuid4())
    
    log_data = [(
        execution_id,
        pipeline_name,
        layer,
        datetime.now(),
        datetime.now() if status != "running" else None,
        status,
        records_count,
        error_msg,
        metadata or {}
    )]
    
    log_df = spark.createDataFrame(log_data, [
        "execution_id", "pipeline_name", "layer", "start_time", 
        "end_time", "status", "records_processed", "error_message", "metadata"
    ])
    
    # Criar database se não existir
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {Config.DATABASE_CONTROL}")
    
    log_df.write.format("delta").mode("append").saveAsTable(f"{Config.DATABASE_CONTROL}.pipeline_execution")
    
    return execution_id

# COMMAND ----------

def unzip_files(source_folder):
    """
    Descompacta arquivos ZIP da Landing Zone
    
    Args:
        source_folder: Nome da pasta no LANDING_RAW (ex: "balancacomercial/EXP")
    
    Returns:
        Lista de arquivos extraídos
    """
    
    if not Config.AUTO_UNZIP:
        print("⏭️  Auto-unzip desabilitado")
        return []
    
    print(f"\n{'='*70}")
    print(f"🔓 DESCOMPACTANDO ARQUIVOS ZIP")
    print(f"{'='*70}")
    
    raw_path = f"{Config.LANDING_RAW}/{source_folder}"
    extract_to = f"{Config.LANDING_EXTRACTED}/{source_folder}"
    processed_path = f"{Config.LANDING_PROCESSED}/{source_folder}"
    
    # Criar diretórios se não existirem
    for path in [extract_to, processed_path]:
        try:
            dbutils.fs.mkdirs(path)
        except:
            pass
    
    # Listar ZIPs
    try:
        files = dbutils.fs.ls(raw_path)
    except:
        print(f"⚠️  Pasta {raw_path} não encontrada")
        return []
    
    zip_files = [f for f in files if f.path.endswith('.zip')]
    
    if not zip_files:
        print(f"ℹ️  Nenhum ZIP encontrado em {raw_path}")
        return []
    
    print(f"📦 Encontrados {len(zip_files)} arquivo(s) ZIP")
    
    extracted_files = []
    
    for zip_info in zip_files:
        try:
            print(f"\n  🔄 Processando: {zip_info.name}")
            
            # Download para /tmp
            local_zip = f"/tmp/{zip_info.name}"
            dbutils.fs.cp(zip_info.path, f"file://{local_zip}")
            
            # Extrair
            with zipfile.ZipFile(local_zip, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                print(f"     📄 {len(file_list)} arquivo(s) dentro do ZIP")
                
                # Extrair cada arquivo
                for file_name in file_list:
                    if file_name.endswith('/'):  # Pular diretórios
                        continue
                    
                    # Extrair para /tmp
                    local_extracted = f"/tmp/extracted/{file_name}"
                    os.makedirs(os.path.dirname(local_extracted), exist_ok=True)
                    
                    with open(local_extracted, 'wb') as f:
                        f.write(zip_ref.read(file_name))
                    
                    # Upload para DBFS
                    target_file = f"{extract_to}/{os.path.basename(file_name)}"
                    dbutils.fs.cp(f"file://{local_extracted}", target_file)
                    extracted_files.append(target_file)
                    
                    print(f"     ✅ {os.path.basename(file_name)} → {extract_to}")
            
            # Mover ZIP para pasta processed
            target_processed = f"{processed_path}/{zip_info.name}"
            dbutils.fs.mv(zip_info.path, target_processed)
            print(f"     📦 ZIP movido para: {processed_path}")
            
            # Limpar /tmp
            os.remove(local_zip)
            
        except Exception as e:
            print(f"     ❌ ERRO ao processar {zip_info.name}: {e}")
            continue
    
    print(f"\n✅ Descompactação concluída: {len(extracted_files)} arquivo(s) extraído(s)")
    
    return extracted_files

# COMMAND ----------

def get_source_paths(source_folder, table_name):
    """
    Retorna lista de paths com wildcards para Autoloader
    
    Args:
        source_folder: Nome da pasta (ex: "EXP")
        table_name: Nome da tabela (ex: "fct_exp")
    
    Returns:
        Lista de paths com wildcards
    """
    
    patterns = Config.FILE_PATTERNS.get(table_name, ["*.csv"])
    base_path = f"{Config.LANDING_EXTRACTED}/{source_folder}"
    
    # Gerar paths com cada pattern
    paths = [f"{base_path}/{pattern}" for pattern in patterns]
    
    return paths

# COMMAND ----------

def get_or_create_table(table_name, schema, layer="bronze", primary_keys=None):
    """
    Cria tabela Delta com Liquid Clustering e CDF se necessário
    SEM Unity Catalog
    """
    
    database = getattr(Config, f"DATABASE_{layer.upper()}")
    full_table_name = f"{database}.{table_name}"
    
    # Criar database se não existir
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    
    if spark.catalog.tableExists(full_table_name):
        print(f"✅ Tabela {full_table_name} já existe")
        return
    
    print(f"📋 Criando tabela {full_table_name}...")
    
    # Schema com metadados
    full_schema = StructType(
        schema.fields + [
            StructField("_ingestion_timestamp", TimestampType(), False),
            StructField("_source_file", StringType(), True),
            StructField("_update_timestamp", TimestampType(), False)
        ]
    )
    
    # Criar DataFrame vazio
    empty_df = spark.createDataFrame([], full_schema)
    
    # Path da tabela
    table_path = f"{getattr(Config, f'{layer.upper()}_PATH')}/{table_name}"
    
    # Configurar write
    writer = empty_df.write.format("delta")
    
    # Liquid Clustering
    if table_name in Config.CLUSTER_CONFIGS:
        cluster_cols = Config.CLUSTER_CONFIGS[table_name]
        print(f"  🔸 Liquid Clustering: {cluster_cols}")
        writer = writer.clusterBy(*cluster_cols)
    
    # Salvar
    writer.mode("overwrite").option("path", table_path).saveAsTable(full_table_name)
    
    # Habilitar CDF
    if table_name in Config.CDF_ENABLED:
        print(f"  🔸 Habilitando CDF...")
        spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    
    print(f"✅ Tabela {full_table_name} criada com sucesso")

# COMMAND ----------

def get_cdf_watermark(table_name, layer="bronze"):
    """
    Busca última versão processada via CDF
    """
    watermark_table = f"{Config.DATABASE_CONTROL}.cdf_watermark"
    
    # Criar tabela se não existir
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {Config.DATABASE_CONTROL}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {watermark_table} (
            table_name STRING,
            layer STRING,
            last_processed_version BIGINT,
            last_processed_timestamp TIMESTAMP,
            updated_at TIMESTAMP
        ) USING DELTA
    """)
    
    try:
        result = spark.sql(f"""
            SELECT last_processed_version, last_processed_timestamp
            FROM {watermark_table}
            WHERE table_name = '{table_name}' AND layer = '{layer}'
        """).first()
        
        if result:
            return result.last_processed_version, result.last_processed_timestamp
        else:
            return 0, None
            
    except Exception as e:
        print(f"⚠️  Watermark não encontrado para {table_name}, iniciando do zero")
        return 0, None

# COMMAND ----------

def update_cdf_watermark(table_name, version, layer="bronze"):
    """
    Atualiza watermark do CDF
    """
    watermark_table = f"{Config.DATABASE_CONTROL}.cdf_watermark"
    
    # MERGE
    spark.sql(f"""
        MERGE INTO {watermark_table} AS target
        USING (
            SELECT 
                '{table_name}' as table_name,
                '{layer}' as layer,
                {version} as last_processed_version,
                current_timestamp() as last_processed_timestamp,
                current_timestamp() as updated_at
        ) AS source
        ON target.table_name = source.table_name AND target.layer = source.layer
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

def get_current_table_version(table_name, layer="bronze"):
    """
    Retorna versão atual da tabela Delta
    """
    database = getattr(Config, f"DATABASE_{layer.upper()}")
    full_table_name = f"{database}.{table_name}"
    
    try:
        history = spark.sql(f"DESCRIBE HISTORY {full_table_name} LIMIT 1").first()
        return history.version if history else 0
    except:
        return 0

# COMMAND ----------



# COMMAND ----------

print("✅ Configurações e utilitários carregados com sucesso!")
print(f"📦 Databases: bronze, silver, gold, control")
print(f"🔧 Liquid Clustering configurado para: {list(Config.CLUSTER_CONFIGS.keys())}")
print(f"📊 CDF habilitado para: {Config.CDF_ENABLED}")
print(f"🔓 Auto-unzip ZIP: {'Habilitado' if Config.AUTO_UNZIP else 'Desabilitado'}")
print(f"🎯 Wildcards configurados para {len(Config.FILE_PATTERNS)} tipos de arquivo")
