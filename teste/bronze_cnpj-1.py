# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze - CNPJ
# MAGIC
# MAGIC **Objetivo**: Ingestão incremental com Autoloader + Liquid Clustering + CDF
# MAGIC
# MAGIC **Melhorias**:
# MAGIC - ✅ Sem Unity Catalog
# MAGIC - ✅ Suporte a wildcards (nomenclatura variável)
# MAGIC - ✅ Descompactação automática de ZIPs
# MAGIC
# MAGIC **Características**:
# MAGIC - Volume: ~50M estabelecimentos
# MAGIC - Mutabilidade: Alta (situação cadastral)
# MAGIC - CDF: Crítico para Silver (rastrear mudanças)

# COMMAND ----------

# MAGIC %run ./config_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("landing_raw_path", f"{Config.LANDING_RAW}/cnpj", "Landing Raw Path")
dbutils.widgets.text("execution_date", datetime.now().strftime("%Y-%m-%d"), "Execution Date")
dbutils.widgets.dropdown("auto_unzip", "true", ["true", "false"], "Auto Unzip ZIPs")

landing_raw_path = dbutils.widgets.get("landing_raw_path")
execution_date = dbutils.widgets.get("execution_date")
Config.AUTO_UNZIP = dbutils.widgets.get("auto_unzip").lower() == "true"

print(f"📂 Landing Raw: {landing_raw_path}")
print(f"📅 Data: {execution_date}")
print(f"🔓 Auto-unzip: {Config.AUTO_UNZIP}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔓 Pré-processamento: Descompactar ZIPs

# COMMAND ----------

# Descompactar ZIPs de todas as pastas CNPJ
folders_to_unzip = [
    "Cnaes", "Motivos", "Municipios", "Naturezas", "Paises", "Qualificacoes",
    "Empresas", "Estabelecimentos", "Socios", "Simples"
]

print("\n" + "="*70)
print("🔓 FASE 1: DESCOMPACTAÇÃO DE ZIPS")
print("="*70)

for folder in folders_to_unzip:
    unzip_files(f"cnpj/{folder}")

print("\n✅ Descompactação concluída!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Ingestão CNPJ

# COMMAND ----------

def ingest_cnpj_autoloader(
    source_folder,
    table_name,
    schema,
    primary_keys,
    encoding="latin-1"
):
    """
    Ingestão CNPJ com MERGE (dados mutáveis) e suporte a wildcards
    """
    
    print(f"\n{'='*70}")
    print(f"🔄 {table_name}")
    print(f"{'='*70}")
    
    exec_id = log_execution(table_name, "bronze", "running")
    
    try:
        # Criar tabela
        get_or_create_table(table_name, schema, "bronze", primary_keys)
        
        # Paths com wildcards
        source_paths = get_source_paths(f"cnpj/{source_folder}", table_name)
        
        print(f"📂 Buscando arquivos com patterns:")
        for path in source_paths:
            print(f"   - {path}")
        
        # Checkpoint e target
        checkpoint = f"{Config.CHECKPOINT_BASE}/bronze/{table_name}"
        target_table = f"{Config.DATABASE_BRONZE}.{table_name}"
        
        # Processar cada pattern
        for source_path in source_paths:
            try:
                # Autoloader
                new_data = (
                    spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "csv")
                    .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
                    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                    .option("sep", ";")
                    .option("header", "false")  # CNPJ não tem header
                    .option("encoding", encoding)
                    .option("quote", '"')
                    .option("escape", '"')
                    .option("maxFilesPerTrigger", Config.MAX_FILES_PER_TRIGGER)
                    .option("cloudFiles.inferColumnTypes", "false")
                    .schema(schema)
                    .load(source_path)
                    .withColumn("_ingestion_timestamp", current_timestamp())
                    .withColumn("_source_file", input_file_name())
                    .withColumn("_update_timestamp", current_timestamp())
                )
                
                # MERGE (upsert para capturar mudanças)
                def merge_batch(batch_df, batch_id):
                    if batch_df.isEmpty():
                        return
                    
                    print(f"  📦 Batch {batch_id}: {batch_df.count():,} registros")
                    
                    merge_condition = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_keys])
                    
                    DeltaTable.forName(spark, target_table).alias("t").merge(
                        batch_df.alias("s"),
                        merge_condition
                    ).whenMatchedUpdateAll(
                    ).whenNotMatchedInsertAll().execute()
                
                query = (
                    new_data.writeStream
                    .foreachBatch(merge_batch)
                    .option("checkpointLocation", checkpoint)
                    .trigger(availableNow=True)
                    .start()
                )
                
                query.awaitTermination()
                
            except Exception as e:
                print(f"  ⚠️  Nenhum arquivo encontrado para pattern {source_path}: {e}")
                continue
        
        # Contar e logar
        count = spark.table(target_table).count()
        log_execution(table_name, "bronze", "success", count)
        
        print(f"✅ {table_name}: {count:,} registros")
        
    except Exception as e:
        log_execution(table_name, "bronze", "failed", error_msg=str(e))
        print(f"❌ ERRO: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Dimensões CNPJ (Simples)

# COMMAND ----------

# Schemas simples (2 colunas)
schema_dim_simples = StructType([
    StructField("codigo", StringType(), False),
    StructField("descricao", StringType(), True)
])

# CNAEs
ingest_cnpj_autoloader(
    source_folder="Cnaes",
    table_name="cnaes",
    schema=schema_dim_simples,
    primary_keys=["codigo"]
)

# Motivos
ingest_cnpj_autoloader(
    source_folder="Motivos",
    table_name="motivos",
    schema=schema_dim_simples,
    primary_keys=["codigo"]
)

# Municípios
ingest_cnpj_autoloader(
    source_folder="Municipios",
    table_name="municipios",
    schema=schema_dim_simples,
    primary_keys=["codigo"]
)

# Naturezas
ingest_cnpj_autoloader(
    source_folder="Naturezas",
    table_name="naturezas",
    schema=schema_dim_simples,
    primary_keys=["codigo"]
)

# Países
ingest_cnpj_autoloader(
    source_folder="Paises",
    table_name="paises",
    schema=schema_dim_simples,
    primary_keys=["codigo"]
)

# Qualificações
ingest_cnpj_autoloader(
    source_folder="Qualificacoes",
    table_name="qualificacoes",
    schema=schema_dim_simples,
    primary_keys=["codigo"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Empresas (Com Liquid Clustering + CDF)

# COMMAND ----------

ingest_cnpj_autoloader(
    source_folder="Empresas",
    table_name="empresas",
    schema=Schemas.empresas,
    primary_keys=["cnpj_basico"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Estabelecimentos (CRÍTICO - Alta Mutabilidade)
# MAGIC
# MAGIC **Atenção**: 
# MAGIC - ~50M registros
# MAGIC - Liquid Clustering: situacao_cadastral, uf, municipio
# MAGIC - CDF habilitado para rastrear mudanças de status

# COMMAND ----------

ingest_cnpj_autoloader(
    source_folder="Estabelecimentos",
    table_name="estabelecimentos",
    schema=Schemas.estabelecimentos,
    primary_keys=["cnpj_basico", "cnpj_ordem", "cnpj_dv"],
    encoding="utf-8"  # Estabelecimentos usam UTF-8
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Sócios (Com CDF)

# COMMAND ----------

ingest_cnpj_autoloader(
    source_folder="Socios",
    table_name="socios",
    schema=Schemas.socios,
    primary_keys=["cnpj_basico", "identificador_socio", "cnpj_cpf_socio"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Simples (Com CDF)

# COMMAND ----------

schema_simples = StructType([
    StructField("cnpj_basico", StringType(), False),
    StructField("opcao_simples", StringType(), True),
    StructField("data_opcao_simples", StringType(), True),
    StructField("data_exclusao_simples", StringType(), True),
    StructField("opcao_mei", StringType(), True),
    StructField("data_opcao_mei", StringType(), True),
    StructField("data_exclusao_mei", StringType(), True)
])

ingest_cnpj_autoloader(
    source_folder="Simples",
    table_name="simples",
    schema=schema_simples,
    primary_keys=["cnpj_basico"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resumo

# COMMAND ----------

print("\n" + "="*70)
print("📊 RESUMO DA CARGA BRONZE - CNPJ")
print("="*70)

tables = [
    ("cnaes", "Dimensão"),
    ("motivos", "Dimensão"),
    ("municipios", "Dimensão"),
    ("naturezas", "Dimensão"),
    ("paises", "Dimensão"),
    ("qualificacoes", "Dimensão"),
    ("empresas", "Entidade Principal"),
    ("estabelecimentos", "Fato (Alta Mutabilidade)"),
    ("socios", "Relacionamento"),
    ("simples", "Atributo Temporal")
]

for table, tipo in tables:
    try:
        count = spark.table(f"{Config.DATABASE_BRONZE}.{table}").count()
        cluster_info = f" [Clustered: {Config.CLUSTER_CONFIGS.get(table, [])}]" if table in Config.CLUSTER_CONFIGS else ""
        cdf_info = " [CDF ✓]" if table in Config.CDF_ENABLED else ""
        print(f"  {table:20} ({tipo:25}) {count:>15,} registros{cluster_info}{cdf_info}")
    except:
        print(f"  {table:20} ({tipo:25}) {'ERROR':>15}")

print("="*70)
print("✅ Bronze concluído!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Estatísticas CDF

# COMMAND ----------

# Mostrar versões das tabelas com CDF
print("\n📊 VERSÕES DAS TABELAS (para controle CDF):\n")

for table in ["empresas", "estabelecimentos", "socios", "simples"]:
    try:
        full_name = f"{Config.DATABASE_BRONZE}.{table}"
        version = get_current_table_version(table, "bronze")
        print(f"  {table:20} -> Versão atual: {version}")
    except:
        print(f"  {table:20} -> Não encontrada")
