# TableDossier — visão geral

**Profiling portátil, notebooks e documentação de dados.**

O TableDossier transforma uma análise exploratória em um *dossiê* estruturado e reprodutível de cada tabela:
estrutura, métricas, formatos observados, sinais de qualidade, limitações e relacionamentos conhecidos.

## Fluxo

```text
SEU COMPUTADOR (sem conexão)         DATABRICKS (suas permissões)                SEU COMPUTADOR (sem conexão)
tabledossier init / generate ─importa─▶ notebook: widgets → validação →  ─copia─▶  tabledossier validate
                                        metadados, amostra, agregações              tabledossier render
                                        → results/<run_id>/profile.json …           → documentação (+ anotações)
```

1. **Gerar localmente**: a CLI cria um notebook `.py` importável, sem acessar banco, Spark ou nuvem.
2. **Executar no Databricks**: o notebook é autossuficiente (não instala nada, não baixa nada) e lê as tabelas com
   as permissões já existentes. Ele grava um pacote de resultados em um diretório novo por execução.
3. **Reutilizar como documentação**: o `profile.json` validado gera dicionário de dados, relatório de qualidade
   (DQR), relacionamentos, diagrama ER em Mermaid e sugestões de regras — de novo offline, sem recalcular métricas.

Um notebook gerado **não contém resultados**; métricas existem apenas depois da execução.

## Princípios

- **Contrato canônico** versionado (JSON Schema 1.2, que só acrescenta ao 1.1, que só acrescenta ao 1.0; a CLI lê
  os três) independente de Spark, Databricks ou PostgreSQL. Cada métrica
  informa `status`, `scope` (escopo), `accuracy` (exatidão) e `source` (origem). "Não calculado" nunca vira zero.
- **Código único**: as células do runtime são os próprios módulos do pacote, copiados de forma legível e
  verificável (com SHA-256), sem payloads codificados.
- **Somente leitura na origem**: nada de `ALTER`, `OPTIMIZE`, `ANALYZE TABLE` ou correção de dados. Delta é lido em
  uma versão fixa (`VERSION AS OF`) em todas as leituras quando possível.
- **Inferência com limites explícitos**: formatos vêm de amostras e dizem isso; contagens distintas aproximadas não
  provam unicidade; relacionamentos nunca são inferidos por nomes; significado de negócio só vem de comentários ou
  anotações humanas.
- **Privacidade por padrão**: valores amostrados, registros e rótulos de valores frequentes não são persistidos.
  Perfis ainda revelam schema, comentários e estatísticas agregadas — não são anonimizados.

## Níveis de análise

- `metadata`: somente metadados de catálogo; nenhuma consulta sobre registros.
- `standard`: uma amostra limitada (apenas colunas texto, para formatos e JSON) e poucas agregações compartilhadas.
- `deep` (aprofundado, parte I): tudo do `standard` mais operações opcionais com orçamento próprio:
  - métricas de **elementos de arrays e entradas de maps** (`items[]`, `items[].sku`, `attrs{key}`,
    `attrs{value}`) calculadas por linha com funções de ordem superior dentro dos passes compartilhados — exatas,
    sem explode, com denominadores em elementos ou entradas (nunca em linhas);
  - **contagens distintas de elementos** em um único passe com explode por tabela, sobre uma amostra limitada ou
    sobre o escopo completo quando ele cabe no orçamento de elementos;
  - **caminhos JSON** de colunas texto a partir da amostra (presença, tipos, heterogeneidade), com validação no
    escopo completo quando o runtime oferece as funções (variant ou `get_json_object`);
  - no máximo `deep.max_extra_passes` ações Spark extras por tabela, independentemente do número de colunas.
- `deep`, parte II (cada verificação é opcional e desligada por padrão):
  - **unicidade exata** de chaves explícitas (inclusive compostas), de PK/UNIQUE declaradas e de candidatas a
    identificador: linhas no escopo, linhas com NULL na chave, chaves distintas, grupos e linhas duplicados, em no
    máximo `deep.uniqueness.max_passes` ações por tabela (as chaves compartilham o passe);
  - **validação referencial** de relacionamentos configurados e de FKs declaradas entre tabelas da mesma execução:
    uma ação por relacionamento, as duas tabelas lidas nas versões Delta registradas, órfãs e sua razão;
    `validated` só no escopo completo sem órfãs, `violated` com órfãs, senão `not_validated` com o motivo;
  - **hipóteses de relacionamento** (desligadas por padrão), vindas só dos dados — nunca de nomes de colunas —,
    separadas dos relacionamentos conhecidos, sem cardinalidade e nunca desenhadas no diagrama ER;
  - só contagens saem do motor: valores duplicados e órfãos nunca são coletados.

## Estado atual

Versão 0.3.0. O runtime foi executado com Spark local 3.5 e 4.0, em modo clássico e por um servidor Spark
Connect local, com dados sintéticos; **ainda não foi validado em um workspace Databricks**. Veja [compatibilidade](../compatibility.md) (em inglês) e o
[roteiro de smoke test](../databricks-smoke-test.md).

Comece pelo [quickstart em português](quickstart.md). A documentação completa está em inglês no
[README](../../README.md).

Licença: Apache-2.0. Seus dados, configurações e relatórios não precisam adotar essa licença.
