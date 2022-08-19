# Cartola

### Conteúdo

1. [Instalação](#installation)
2. [Motivação do Projeto](#motivation)
3. [Descrição dos Arquivos](#files)
4. [Resultados](#results)
5. [Instruções](#instructions)
6. [Referências](#licensing)

## Instalação <a name="installation"></a>

Pré-requisitos:
1. Docker Desktop
2. Neo4j Desktop

Imagens Docker:
1. Jupyter-lab + Spark
    ```console
    docker run --name spark -p 4040:4040 -p 8888:8888 -p 8501:8501 -v <caminho para salvar localmente>:/home/jovyan/work jupyter/all-spark-notebook
    ```
2. Couchbase
    ```console
    docker run -d --name couchbase -p 8091-8096:8091-8096 -p 11210-11211:11210-11211 -v <caminho para salvar localmente>:/opt/couchbase/var couchbase
    ```

Pacotes Python:
1. pyspark
2. couchbase
3. streamlit

## Motivação do Projeto<a name="motivation"></a>

TBD

## Descrição dos Arquivos <a name="files"></a>

TBD

## Resultados<a name="results"></a>

TBD

### Instruções<a name="instructions"></a>

TBD

## Referências<a name="licensing"></a>

Fonte dos dados: https://github.com/henriquepgomide/caRtola