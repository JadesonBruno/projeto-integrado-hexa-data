# 🔄 Refatoração: Suporte a ZIP + Wildcards + Sem Unity Catalog

## 📋 Resumo das Mudanças

Este documento descreve as melhorias implementadas no pipeline para:
1. ✅ Funcionar **sem Unity Catalog** (usa databases tradicionais)
2. ✅ Suportar **nomenclatura variável** de arquivos (wildcards)
3. ✅ **Descompactar ZIPs automaticamente**

---

## 🎯 Principais Mudanças

### **1. Remoção do Unity Catalog**

**Antes:**
```python
CATALOG = "comercio_exterior"
SCHEMA_BRONZE = "bronze"
target_table = f"{Config.CATALOG}.{Config.SCHEMA_BRONZE}.{table_name}"
# Exemplo: "comercio_exterior.bronze.fct_exp"
```

**Depois:**
```python
DATABASE_BRONZE = "bronze"
target_table = f"{Config.DATABASE_BRONZE}.{table_name}"
# Exemplo: "bronze.fct_exp"
```

**Comandos de criação:**
```sql
-- Antes (Unity Catalog)
CREATE SCHEMA comercio_exterior.bronze;

-- Depois (Database tradicional)
CREATE DATABASE IF NOT EXISTS bronze;
```

---

### **2. Suporte a Wildcards (Nomenclatura Variável)**

**Problema resolvido:**
```
Janeiro: EXP_2025_01.csv
Fevereiro: EXPORTACAO_202502.csv  ← Nome diferente!
Março: exp_marco_2025.csv          ← Totalmente diferente!
```

**Solução:**
```python
FILE_PATTERNS = {
    "fct_exp": [
        "EXP*.csv",          # Pega EXP_2025_01.csv
        "EXPORTACAO*.csv",   # Pega EXPORTACAO_202502.csv
        "exp*.csv",          # Pega exp_marco_2025.csv
        "exportacao*.csv"
    ],
    "estabelecimentos": [
        "*.ESTABELE",         # Formato original Receita Federal
        "*ESTABELE*.csv",     # Variações
        "*estabelecimento*.csv"
    ]
}
```

**Como funciona:**
- Autoloader tenta cada pattern sequencialmente
- Processa qualquer arquivo que corresponda a qualquer pattern
- Checkpoint garante que não duplica dados

---

### **3. Descompactação Automática de ZIPs**

**Estrutura de Landing Zone:**

```
/mnt/landing/
├── raw/                           ← ZIPs chegam aqui
│   └── cnpj/
│       ├── Empresas/
│       │   └── dados_empresas.zip
│       └── Estabelecimentos/
│           └── estabelecimentos_01.zip
│
├── extracted/                     ← CSVs extraídos (Autoloader lê)
│   └── cnpj/
│       ├── Empresas/
│       │   ├── F.K032001K.D11101.EMPRECSV
│       │   └── F.K032001K.D11102.EMPRECSV
│       └── Estabelecimentos/
│           └── K3241.K03200Y0.D10810.ESTABELE
│
└── processed/                     ← ZIPs movidos após extração
    └── cnpj/
        └── Empresas/
            └── dados_empresas.zip
```

**Fluxo automático:**

1. ZIP é detectado em `/mnt/landing/raw/`
2. Função `unzip_files()` extrai para `/mnt/landing/extracted/`
3. Autoloader processa CSVs em `/extracted/`
4. ZIP é movido para `/mnt/landing/processed/`

**Controle:**
```python
# Habilitar/desabilitar via widget
dbutils.widgets.dropdown("auto_unzip", "true", ["true", "false"], "Auto Unzip")

# Ou via configuração
Config.AUTO_UNZIP = True  # ou False
```

---

## 📁 Arquivos Modificados

### **config_utils.py**
- ✅ Removido `CATALOG`, adicionado `DATABASE_BRONZE/SILVER/GOLD`
- ✅ Adicionado `FILE_PATTERNS` com wildcards
- ✅ Nova função `unzip_files()` para descompactar ZIPs
- ✅ Nova função `get_source_paths()` para gerar paths com wildcards
- ✅ Função `get_or_create_table()` usa paths DBFS em vez de Unity Catalog

### **bronze_balanca_comercial.py**
- ✅ Nova fase de descompactação antes do Autoloader
- ✅ `ingest_with_autoloader()` usa múltiplos patterns
- ✅ Schema evolution habilitado: `.option("cloudFiles.schemaEvolutionMode", "addNewColumns")`

### **bronze_cnpj.py**
- ✅ Descompactação de ZIPs da Receita Federal
- ✅ Suporte a patterns variáveis (`.EMPRECSV`, `*empresa*.csv`, etc.)
- ✅ Encoding UTF-8 para Estabelecimentos

### **silver_incremental.py**
- ✅ Referências ao Unity Catalog substituídas por databases
- ✅ Sem mudanças na lógica CDF

### **gold_agregacoes.py**
- ✅ Referências ao Unity Catalog substituídas por databases
- ✅ Sem mudanças na lógica de agregação

### **data_quality_validation.py**
- ✅ Referências ao Unity Catalog substituídas por databases

---

## 🚀 Como Usar

### **Setup Inicial**

```sql
-- Criar databases
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;
CREATE DATABASE IF NOT EXISTS control;

-- Criar tabelas de controle
CREATE TABLE IF NOT EXISTS control.pipeline_execution (
    execution_id STRING,
    pipeline_name STRING,
    layer STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,
    records_processed BIGINT,
    error_message STRING,
    metadata MAP<STRING, STRING>
) USING DELTA;

CREATE TABLE IF NOT EXISTS control.cdf_watermark (
    table_name STRING,
    layer STRING,
    last_processed_version BIGINT,
    last_processed_timestamp TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;
```

### **Estrutura de Pastas (DBFS)**

```bash
# Criar estrutura de landing
dbutils.fs.mkdirs("/mnt/landing/raw/balancacomercial")
dbutils.fs.mkdirs("/mnt/landing/raw/cnpj")
dbutils.fs.mkdirs("/mnt/landing/extracted")
dbutils.fs.mkdirs("/mnt/landing/processed")

# Criar pastas de dados
dbutils.fs.mkdirs("/mnt/datalake/bronze")
dbutils.fs.mkdirs("/mnt/datalake/silver")
dbutils.fs.mkdirs("/mnt/datalake/gold")

# Criar pastas de checkpoint
dbutils.fs.mkdirs("/mnt/checkpoints")
```

### **Upload de Arquivos**

**Opção 1: Arquivos CSV direto**
```python
# Copiar CSVs para a pasta extracted (pula etapa de unzip)
dbutils.fs.cp("file:/local/EXP_2025_01.csv", "/mnt/landing/extracted/balancacomercial/EXP/")

# Executar Bronze (auto-unzip desabilitado)
%run ./bronze_balanca_comercial
```

**Opção 2: Arquivos ZIP**
```python
# Copiar ZIPs para pasta raw
dbutils.fs.cp("file:/local/dados.zip", "/mnt/landing/raw/balancacomercial/EXP/")

# Executar Bronze (auto-unzip habilitado - padrão)
%run ./bronze_balanca_comercial
# O notebook automaticamente:
# 1. Detecta o ZIP
# 2. Descompacta para /extracted/
# 3. Processa com Autoloader
# 4. Move ZIP para /processed/
```

### **Execução Manual**

```python
# Bronze Balança Comercial
%run ./bronze_balanca_comercial

# Bronze CNPJ
%run ./bronze_cnpj

# Silver (processamento incremental via CDF)
%run ./silver_incremental

# Gold (agregações incrementais)
%run ./gold_agregacoes

# Validação
%run ./data_quality_validation
```

---

## 🎛️ Configurações Importantes

### **Habilitar/Desabilitar Auto-Unzip**

**Via Widget (na execução):**
```python
dbutils.widgets.dropdown("auto_unzip", "true", ["true", "false"], "Auto Unzip")
```

**Via Config (hard-coded):**
```python
# Em config_utils.py
Config.AUTO_UNZIP = False  # Desabilitar descompactação
```

### **Adicionar Novos Wildcards**

```python
# Em config_utils.py
FILE_PATTERNS = {
    "fct_exp": [
        "EXP*.csv",
        "EXPORTACAO*.csv",
        "NOVO_PADRAO*.csv"  # ← Adicione aqui
    ]
}
```

### **Schema Evolution**

Se as colunas dos CSVs mudarem:

```python
# Já habilitado no código
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
.option("mergeSchema", "true")

# Comportamento:
# - Novas colunas: Adicionadas automaticamente (preenchidas com NULL nos registros antigos)
# - Colunas removidas: Ignoradas (dados antigos mantidos)
# - Tipos diferentes: Usa tipo mais genérico (ex: INT → STRING)
```

---

## 🧪 Testes

### **Teste 1: Nomenclatura Variável**

```python
# Criar arquivos com nomes diferentes
dbutils.fs.put("/mnt/landing/extracted/balancacomercial/EXP/EXP_2025_01.csv", "CO_ANO;CO_MES;VL_FOB\n2025;01;1000")
dbutils.fs.put("/mnt/landing/extracted/balancacomercial/EXP/EXPORTACAO_202502.csv", "CO_ANO;CO_MES;VL_FOB\n2025;02;2000")
dbutils.fs.put("/mnt/landing/extracted/balancacomercial/EXP/exp_marco.csv", "CO_ANO;CO_MES;VL_FOB\n2025;03;3000")

# Executar Bronze
%run ./bronze_balanca_comercial

# Verificar
spark.sql("SELECT * FROM bronze.fct_exp ORDER BY CO_MES").show()
# Deve mostrar 3 registros (janeiro, fevereiro, março)
```

### **Teste 2: Descompactação de ZIP**

```python
# Criar ZIP localmente
import zipfile
with zipfile.ZipFile('/tmp/teste.zip', 'w') as z:
    z.writestr('EXP_2025_04.csv', 'CO_ANO;CO_MES;VL_FOB\n2025;04;4000')

# Upload para raw
dbutils.fs.cp("file:///tmp/teste.zip", "/mnt/landing/raw/balancacomercial/EXP/")

# Executar Bronze
%run ./bronze_balanca_comercial

# Verificar:
# 1. ZIP foi movido para /processed/
# 2. CSV extraído para /extracted/
# 3. Dados ingeridos no Bronze
spark.sql("SELECT * FROM bronze.fct_exp WHERE CO_MES = '04'").show()
```

### **Teste 3: Idempotência**

```python
# Executar Bronze 2x seguidas
%run ./bronze_balanca_comercial
count1 = spark.sql("SELECT COUNT(*) FROM bronze.fct_exp").collect()[0][0]

%run ./bronze_balanca_comercial
count2 = spark.sql("SELECT COUNT(*) FROM bronze.fct_exp").collect()[0][0]

# count1 deve ser igual a count2 (não duplicou)
assert count1 == count2, "Pipeline não é idempotente!"
```

---

## ⚠️ Troubleshooting

### **Erro: "Database does not exist"**

```sql
-- Criar databases manualmente
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;
CREATE DATABASE IF NOT EXISTS control;
```

### **Erro: "Path does not exist" no unzip**

```python
# Criar estrutura de landing
dbutils.fs.mkdirs("/mnt/landing/raw")
dbutils.fs.mkdirs("/mnt/landing/extracted")
dbutils.fs.mkdirs("/mnt/landing/processed")
```

### **Arquivos não sendo processados**

```python
# Verificar se arquivos existem
dbutils.fs.ls("/mnt/landing/extracted/balancacomercial/EXP/")

# Resetar checkpoint (última opção)
dbutils.fs.rm("/mnt/checkpoints/bronze/fct_exp", recurse=True)

# Re-executar
%run ./bronze_balanca_comercial
```

### **ZIP não descompacta**

```python
# Verificar se auto-unzip está habilitado
print(Config.AUTO_UNZIP)  # Deve ser True

# Executar descompactação manualmente
unzip_files("balancacomercial/EXP")
```

---

## 📊 Comparação: Antes vs Depois

| Aspecto | Antes | Depois |
|---------|-------|--------|
| **Unity Catalog** | Obrigatório | ❌ Não usa |
| **Nomenclatura fixa** | EXP_2025_01.csv | ✅ EXP*, EXPORTACAO*, exp* |
| **ZIP** | Manual | ✅ Automático |
| **Schema changes** | Erro | ✅ Evolution automático |
| **Complexity** | Alta | ✅ Baixa |

---

## ✅ Checklist de Implementação

- [ ] Criar databases (`bronze`, `silver`, `gold`, `control`)
- [ ] Criar estrutura de landing (`/raw`, `/extracted`, `/processed`)
- [ ] Upload dos notebooks refatorados
- [ ] Configurar wildcards para seus padrões de arquivo
- [ ] Testar descompactação de ZIP
- [ ] Testar nomenclatura variável
- [ ] Validar idempotência
- [ ] Configurar Workflow (se aplicável)
- [ ] Documentar padrões de nomenclatura da equipe

---

## 🎯 Conclusão

O pipeline agora é:
- ✅ **Mais flexível**: Aceita qualquer nome de arquivo
- ✅ **Mais simples**: Sem Unity Catalog
- ✅ **Mais automático**: Descompacta ZIPs sozinho
- ✅ **Mais robusto**: Schema evolution habilitado
- ✅ **Mesmo desempenho**: CDF + Liquid Clustering intactos

**Compatibilidade total com atualização mensal incremental!** 🚀
