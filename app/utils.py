from pyspark.sql import SparkSession
import pyspark.pandas as ps
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions

def carregar_rodada(rodada):
    # Conexão com o Spark
    spark = SparkSession.builder.master("local").getOrCreate()
    
    # Carrega rodada do arquivo CSV
    df = spark.read.csv(f'../data/rodada-{rodada}.csv', header=True)
    
    columns = [
        '`atletas.atleta_id`',
        '`atletas.rodada_id`',
        '`atletas.posicao_id`',
        '`atletas.status_id`',
        '`atletas.pontos_num`',
        '`atletas.preco_num`',
        '`atletas.variacao_num`',
        '`atletas.media_num`',
        '`atletas.jogos_num`',
        '`atletas.minimo_para_valorizar`',
        '`atletas.apelido`',
        '`atletas.clube.id.full.name`',
        '`DE`',
        '`SG`',
        '`GS`',
        '`CA`',
        '`FD`',
        '`FF`',
        '`FS`',
        '`G`',
        '`I`',
        '`PI`',
        '`DS`',
        '`FC`',
        '`A`',
        '`CV`',
        '`FT`',
        '`PP`',
        '`GC`',
        '`PS`',
        '`PC`'
    ]

    # Remove colunas que não serão úteis para a aplicação
    df = df.select(columns)
    
    # Renomeia as colunas
    newColumns = [
        'atleta_id',
        'rodada_id',
        'posicao_id',
        'status_id',
        'pontos',
        'preco',
        'preco_variacao',
        'pontos_media',
        'jogos',
        'minimo_para_valorizar',
        'jogador',
        'clube',
        'defesas',
        'jogos_sem_sofrer_gol',
        'gols_sofridos',
        'cartoes_amarelos',
        'finalizacoes_defendidas',
        'finalizacoes_fora',
        'faltas_sofridas',
        'gols',
        'impedimentos',
        'passes_incompletos',
        'desarmes',
        'faltas_cometidas',
        'assistencias',
        'cartoes_vermelhos',
        'finalizacoes_trave',
        'penaltis_perdidos',
        'gols_contra',
        'penaltis_sofridos',
        'penaltis_cometidos'
    ]
    df = df.toDF(*newColumns)
    
    return df

def upload_couchbase(df):
    # Conecta no Couchbase
    auth = PasswordAuthenticator('admin', 'admin123')
    cluster = Cluster('couchbase://host.docker.internal', ClusterOptions(auth))
    coll = cluster.bucket('cartola').scope('cartola').collection('atletas')
    
    # Tranforma o dataframe em dicionários
    key = df.select('atleta_id', 'rodada_id').toPandas().to_dict(orient='records')
    doc = df.drop(*['atleta_id', 'rodada_id']).toPandas().to_dict(orient='records')
    
    # Carrega os dados no Couchbase
    for k, d in zip(key, doc):
        coll.upsert(k['atleta_id'] + '|' + k['rodada_id'], d)
        
    try:
        cluster.query("CREATE PRIMARY INDEX ON `{}`.`{}`.{}".format("cartola","cartola","atletas")).execute()
    except:
        pass
    
def query_couchbase(query):
    # Conecta no Couchbase
    auth = PasswordAuthenticator('admin', 'admin123')
    cluster = Cluster('couchbase://host.docker.internal', ClusterOptions(auth))
    coll = cluster.bucket('cartola').scope('cartola').collection('atletas')
    
    # Executa a consulta
    row_iter = cluster.query(query)
    df = ps.DataFrame(list(row_iter))
    
    try:
        df[['atleta_id', 'rodada_id']] = df['id'].str.split('|', 1, expand=True)
        df = df.drop('id', axis=1)
    except:
        pass
    
    return df