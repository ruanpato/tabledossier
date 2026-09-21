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

- **Contrato canônico** versionado (JSON Schema 1.0) independente de Spark, Databricks ou PostgreSQL. Cada métrica
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
- `deep` (aprofundado): planejado, não disponível.

## Estado atual

Versão 0.1.0. O runtime foi executado com Spark local 3.5 e 4.0 e dados sintéticos; **ainda não foi validado em
um workspace Databricks**. Veja [compatibilidade](../compatibility.md) (em inglês) e o
[roteiro de smoke test](../databricks-smoke-test.md).

Comece pelo [quickstart em português](quickstart.md). A documentação completa está em inglês no
[README](../../README.md).

Licença: Apache-2.0. Seus dados, configurações e relatórios não precisam adotar essa licença.
