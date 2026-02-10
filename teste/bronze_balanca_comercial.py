# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze - Balança Comercial
# MAGIC
# MAGIC **Objetivo**: Ingestão incremental com Autoloader + Liquid Clustering + CDF
# MAGIC
# MAGIC **Melhorias**:
# MAGIC - ✅ Sem Unity Catalog
# MAGIC - ✅ Suporte a wildcards (nomenclatura variável)
# MAGIC - ✅ Descompactação automática de ZIPs
# MAGIC
# MAGIC **Processamento**:
# MAGIC - Dimensões: MERGE (upsert)
# MAGIC - Fatos: APPEND (apenas novos períodos)

# COMMAND ----------

# MAGIC %run ./config_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("landing_raw_path", f"{Config.LANDING_RAW}/balancacomercial", "Landing Raw Path")
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

# Descompactar ZIPs de todas as pastas relevantes
folders_to_unzip = [
    "PAIS", "VIA", "URF", "NCM_UNIDADE", "UF", "NCM", 
    "UF_MUN", "EXP", "IMP", "EXP_MUN", "IMP_MUN"
]

print("\n" + "="*70)
print("🔓 FASE 1: DESCOMPACTAÇÃO DE ZIPS")
print("="*70)

for folder in folders_to_unzip:
    unzip_files(f"balancacomercial/{folder}")

print("\n✅ Descompactação concluída!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Ingestão com Wildcards

# COMMAND ----------

def ingest_with_autoloader(
    source_folder,
    table_name,
    schema,
    primary_keys,
    encoding="latin-1",
    is_fact=False
):
    """
    Ingestão incremental com suporte a wildcards
    
    Args:
        source_folder: Nome da pasta no landing (ex: "EXP")
        table_name: Nome da tabela (ex: "fct_exp")
        schema: Schema Spark
        primary_keys: Lista de PKs
        encoding: Encoding do CSV
        is_fact: Se True, usa APPEND; se False, usa MERGE
    """
    
    print(f"\n{'='*70}")
    print(f"🔄 {table_name}")
    print(f"{'='*70}")
    
    exec_id = log_execution(table_name, "bronze", "running")
    
    try:
        # Criar tabela se não existir
        get_or_create_table(table_name, schema, "bronze", primary_keys)
        
        # Paths com wildcards (múltiplos patterns)
        source_paths = get_source_paths(f"balancacomercial/{source_folder}", table_name)
        
        print(f"📂 Buscando arquivos com patterns:")
        for path in source_paths:
            print(f"   - {path}")
        
        # Checkpoint e target
        checkpoint = f"{Config.CHECKPOINT_BASE}/bronze/{table_name}"
        target_table = f"{Config.DATABASE_BRONZE}.{table_name}"
        
        # Ler com Autoloader (aceita lista de paths)
        new_data = spark.createDataFrame([], schema)  # DataFrame vazio inicial
        
        for source_path in source_paths:
            try:
                stream_df = (
                    spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "csv")
                    .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
                    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Schema evolution
                    .option("sep", ";")
                    .option("header", "true")
                    .option("encoding", encoding)
                    .option("quote", '"')
                    .option("escape", '"')
                    .option("maxFilesPerTrigger", Config.MAX_FILES_PER_TRIGGER)
                    .schema(schema)
                    .load(source_path)
                )
                
                # Adicionar metadados
                stream_df = (
                    stream_df
                    .withColumn("_ingestion_timestamp", current_timestamp())
                    .withColumn("_source_file", input_file_name())
                    .withColumn("_update_timestamp", current_timestamp())
                )
                
                # Processamento por tipo
                if is_fact:
                    # FATOS: Append direto
                    query = (
                        stream_df.writeStream
                        .format("delta")
                        .outputMode("append")
                        .option("checkpointLocation", checkpoint)
                        .trigger(availableNow=True)
                        .toTable(target_table)
                    )
                else:
                    # DIMENSÕES: MERGE (upsert)
                    def merge_batch(batch_df, batch_id):
                        if batch_df.isEmpty():
                            return
                        
                        merge_condition = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_keys])
                        
                        DeltaTable.forName(spark, target_table).alias("t").merge(
                            batch_df.alias("s"),
                            merge_condition
                        ).whenMatchedUpdate(
                            condition="s._ingestion_timestamp > t._ingestion_timestamp",
                            set={col: f"s.{col}" for col in batch_df.columns if not col.startswith("_")} | 
                                {"_update_timestamp": "s._update_timestamp"}
                        ).whenNotMatchedInsertAll().execute()
                    
                    query = (
                        stream_df.writeStream
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
# MAGIC ## 1️⃣ Dimensões Simples

# COMMAND ----------

# Pais
ingest_with_autoloader(
    source_folder="PAIS",
    table_name="dim_pais",
    schema=Schemas.dim_pais,
    primary_keys=["CO_PAIS"]
)

# Via
ingest_with_autoloader(
    source_folder="VIA",
    table_name="dim_via",
    schema=Schemas.dim_via,
    primary_keys=["CO_VIA"]
)

# URF
ingest_with_autoloader(
    source_folder="URF",
    table_name="dim_ufr",
    schema=Schemas.dim_ufr,
    primary_keys=["CO_URF"]
)

# Unidade
ingest_with_autoloader(
    source_folder="NCM_UNIDADE",
    table_name="dim_ncm_unidade",
    schema=Schemas.dim_ncm_unidade,
    primary_keys=["CO_UNID"]
)

# UF
ingest_with_autoloader(
    source_folder="UF",
    table_name="dim_uf",
    schema=Schemas.dim_uf,
    primary_keys=["SG_UF"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Dimensões Complexas

# COMMAND ----------

# NCM (com Liquid Clustering)
ingest_with_autoloader(
    source_folder="NCM",
    table_name="dim_ncm",
    schema=Schemas.dim_ncm,
    primary_keys=["CO_NCM"]
)

# Municípios (com Liquid Clustering)
ingest_with_autoloader(
    source_folder="UF_MUN",
    table_name="dim_uf_mun",
    schema=Schemas.dim_uf_mun,
    primary_keys=["CO_MUN_GEO"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Fatos (Append Only + Liquid Clustering + CDF)

# COMMAND ----------

# Exportação Nacional
ingest_with_autoloader(
    source_folder="EXP",
    table_name="fct_exp",
    schema=Schemas.fct_exp,
    primary_keys=["CO_ANO", "CO_MES", "CO_NCM", "CO_PAIS"],
    is_fact=True
)

# Importação Nacional
ingest_with_autoloader(
    source_folder="IMP",
    table_name="fct_imp",
    schema=Schemas.fct_imp,
    primary_keys=["CO_ANO", "CO_MES", "CO_NCM", "CO_PAIS"],
    is_fact=True
)

# Exportação Municipal
ingest_with_autoloader(
    source_folder="EXP_MUN",
    table_name="fct_exp_mun",
    schema=Schemas.fct_exp_mun,
    primary_keys=["CO_ANO", "CO_MES", "SH4", "CO_PAIS", "CO_MUN"],
    is_fact=True
)

# Importação Municipal
ingest_with_autoloader(
    source_folder="IMP_MUN",
    table_name="fct_imp_mun",
    schema=Schemas.fct_imp_mun,
    primary_keys=["CO_ANO", "CO_MES", "SH4", "CO_PAIS", "CO_MUN"],
    is_fact=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resumo

# COMMAND ----------

print("\n" + "="*70)
print("📊 RESUMO DA CARGA BRONZE - BALANÇA COMERCIAL")
print("="*70)

tables = [
    "dim_pais", "dim_via", "dim_ufr", "dim_ncm_unidade", "dim_uf",
    "dim_ncm", "dim_uf_mun",
    "fct_exp", "fct_imp", "fct_exp_mun", "fct_imp_mun"
]

for table in tables:
    try:
        count = spark.table(f"{Config.DATABASE_BRONZE}.{table}").count()
        cluster_info = f" [Clustered: {Config.CLUSTER_CONFIGS.get(table, [])}]" if table in Config.CLUSTER_CONFIGS else ""
        cdf_info = " [CDF ✓]" if table in Config.CDF_ENABLED else ""
        print(f"  {table:25} {count:>15,} registros{cluster_info}{cdf_info}")
    except:
        print(f"  {table:25} {'ERROR':>15}")

print("="*70)
print("✅ Bronze concluído!")
