# Quickstart (português)

## 1. Instalar a CLI (no seu computador)

O pacote ainda não está publicado no PyPI. Instale a tag da versão no GitHub, ou a partir do repositório clonado
com `python -m pip install .`; para máquinas sem internet, veja [instalação offline](../offline-install.md):

```bash
python -m venv .venv
# Ative o ambiente conforme o sistema operacional (ex.: source .venv/bin/activate).
python -m pip install "tabledossier @ git+https://github.com/ruanpato/tabledossier@v0.4.0"
```

Requisitos locais: Python 3.10 ou superior. Não é preciso Spark, Java, Docker, drivers de banco, credenciais nem
SDKs de nuvem.

## 2. Criar a configuração e gerar o notebook (sem conexão)

```bash
tabledossier init --output profile.config.json
tabledossier validate --config profile.config.json
tabledossier generate --config profile.config.json --output dist/profile_databricks.py
```

O `init` grava todas as opções com seus valores padrão (documentadas em [configuração](../configuration.md)).
A lista de tabelas pode ficar vazia: o notebook é gerado mesmo assim e pede as tabelas na execução. Nunca coloque
segredos na configuração.

## 3. Executar no Databricks

1. **Importe** `dist/profile_databricks.py` no workspace (pasta → ⋮ → *Import* → *File*).
2. **Associe um compute** com Databricks Runtime compatível (alvo principal: 16.4 LTS).
3. **Dados de demonstração (opcional)**: execute [`examples/demo/create_demo_tables.sql`](../../examples/demo/create_demo_tables.sql)
   em um schema seu, por exemplo `demo.analytics` (cria quatro tabelas sintéticas).
4. **Preencha os widgets** no topo:
   - `tables_json`: por exemplo `["demo.analytics.orders"]`;
   - `output_dir`: um diretório em que você pode gravar, por exemplo `/Volumes/<catalogo>/<schema>/<volume>/tabledossier`;
   - `analysis_level`: `standard` (padrão), `metadata` ou `deep` (veja abaixo);
   - `config_json`: ajustes opcionais em JSON (filtros, limites, checks), sem segredos.

   Precedência: padrões internos < configuração gerada < `config_json` < widgets dedicados.
5. Clique em *Run all*. A célula de exportação mostra o caminho do pacote e um comando para copiá-lo, por exemplo
   `databricks fs cp -r dbfs:/Volumes/<catalogo>/<schema>/<volume>/tabledossier/<run_id> ./downloaded/<run_id>`
   (também é possível baixar pelo Catalog Explorer).

Uma tabela com erro (por exemplo, inexistente) recebe status `failed` com mensagem sanitizada; as demais continuam e
a execução fica `partial`.

A última célula encerra o notebook com `dbutils.notebook.exit` e um resumo JSON pequeno (id da execução, status,
diretório de resultados e contagens), que aparece como "Notebook exited". Tudo já foi gravado antes dela. Para
desligar, use `config_json` = `{"jobs": {"exit_summary": false}}`.

### Como Job do Databricks

Passe `tables_json`, `analysis_level`, `output_dir` e `config_json` como parâmetros da tarefa de notebook: eles chegam
como valores dos widgets e o notebook não os sobrescreve. O resumo JSON da última célula fica disponível para quem
executou o notebook (`dbutils.notebook.run`, API de Jobs `runs/get-output`). A execução como Job ainda não foi
validada em um workspace; detalhes e um exemplo de definição (não validado) em
[execução como Job](../databricks-jobs.md) (em inglês).

### Nível `deep` (opcional)

Com `analysis_level` = `deep`, o notebook também perfila elementos de arrays e entradas de maps e cataloga caminhos
JSON de colunas texto. Tudo tem orçamento na seção `deep` da configuração (ou em `config_json`), por exemplo:

```json
{"deep": {"targets": [{"table": "demo.analytics.orders", "column": "items"}], "max_extra_passes": 1}}
```

- `targets`: `"all_within_budget"` (padrão: todos os arrays, maps e colunas com JSON provável) ou uma lista
  explícita de tabela e coluna;
- `max_extra_passes` (2): ações Spark extras permitidas por tabela;
- `element_distinct` (`sample`): contagens distintas de elementos sobre uma amostra de até `max_explode_rows` (1000)
  linhas e `max_elements` (100000) elementos, `full_scope` ou `off`;
- `max_json_paths` (50) e `max_json_depth` (3): limites do catálogo de caminhos JSON.

Os denominadores das métricas de elementos são elementos ou entradas, nunca linhas. O catálogo de caminhos JSON vem
da amostra e não é um schema completo. O relatório de qualidade mostra o que o nível deep cobriu e o que ficou
limitado pelo orçamento. Detalhes em [configuração](../configuration.md#deep) (em inglês).

### Unicidade, integridade referencial e hipóteses (nível `deep`, parte II)

Cada verificação é opcional, tem orçamento e aparece nas operações planejadas do perfil. Exemplo de `config_json`:

```json
{"deep": {
  "uniqueness": {"keys": [{"table": "demo.analytics.orders", "columns": ["order_id"]}], "declared_keys": true},
  "referential": {"configured": true, "declared": true},
  "relationship_hypotheses": {"enabled": true}
}}
```

- `uniqueness`: chaves explícitas (listas de colunas, inclusive compostas), `declared_keys` (PK/UNIQUE do Unity
  Catalog), `identifier_candidates`, `max_keys` (5) e `max_passes` (1) por tabela. Linhas com NULL em alguma coluna
  da chave são contadas à parte e nunca contam como duplicadas.
- `referential`: `configured` (relacionamentos da configuração), `declared` (FKs do Unity Catalog), `mode`
  (`full_scope` ou `sample`, com `max_sample_rows`) e `max_relationships` (5) por execução. As duas tabelas precisam
  estar na mesma execução; a origem mantém o escopo filtrado e o destino é lido inteiro, nas versões registradas.
  Uma amostra só pode provar violação, nunca validar.
- `relationship_hypotheses`: desligadas por padrão; `max_pairs` (5), `max_sample_rows`, `inclusion_scope` e
  `min_inclusion_ratio` (0.95). Os pares vêm de tipos compatíveis e de faixas de valores já medidas, nunca de nomes.

O DQR ganha as seções "Uniqueness (exact)" e "Referential integrity"; o `relationships.md` mostra o status da
validação com evidência e uma seção de hipóteses. Valores duplicados e órfãos nunca são persistidos. Detalhes em
[configuração](../configuration.md#deepuniqueness) (em inglês).

## 4. Validar e regenerar a documentação (sem conexão)

```bash
tabledossier validate --profile downloaded/profile.json
tabledossier render --input downloaded/profile.json --output docs/generated
```

Para incluir descrições, responsáveis e relacionamentos escritos por pessoas, use
`--annotations annotations.json` ([exemplo](../../examples/demo/annotations.json)). As anotações ficam separadas e
nunca são sobrescritas pelo conteúdo gerado.

### Demonstração local do renderer

Este comando apenas renderiza o perfil sintético incluído no repositório; **não executa profiling contra nenhum banco**:

```bash
tabledossier render --input examples/demo/output/run/profile.json \
  --annotations examples/demo/annotations.json --output docs/generated/demo
```

## Arquivos gerados

| Arquivo | Conteúdo |
| --- | --- |
| `manifest.json` | status da execução, hashes dos arquivos, resultado da validação |
| `profile.json` | perfil canônico (contrato 1.2) — fonte de todos os documentos |
| `overview.md` | execução, ambiente, tabelas, amostragem, erros |
| `data_dictionary.md` | dicionário de dados com origem de cada descrição |
| `quality_report.md` | DQR: checks executados, completude, alertas, propostas e limitações |
| `relationships.md`, `erd.mmd` | relacionamentos conhecidos e diagrama ER |
| `suggested_rules.json` | sugestões de regras (nunca aplicadas automaticamente) |

## Códigos de saída da CLI

`0` sucesso · `1` erro interno · `2` uso incorreto · `3` entrada inválida ou versão incompatível · `4` erro de E/S ou
saída já existente · `5` perfil válido de execução parcial · `6` perfil válido de execução com falha · `7` checks
configurados falharam (com `--fail-on-check-failures`).
