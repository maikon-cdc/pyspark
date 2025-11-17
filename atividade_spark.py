# a linha abaixo importa a classe sparksession do modulo pyspark.sql
# no phyton, a palavra chave "from" é usada para indicar de onde vamos trazer algo.
# aqui embaixo estamos dizendo: do modulo pyspark.sql traga "import" apenas a sparksession 
# o sparksession é uma classe fundamental para trabalhar com o spark pyspark
# ele é como a porta de entrada para usar todas as funções do spark
from pyspark.sql import SparkSession

# agora importamos duas funções: avg (calcular a média) e count(para contar registros)
# essas funções ficam dentro do modulo pyspark.sql.functions
# elas permitem que façamos calculos diretamente em colunas de tabelas do spark.
from pyspark.sql.functions import avg, count

# aqui criamos a variavel chamada spark. poderiamos dar outro nome,
# porém é uma convenção mundial chamar essa variavel de spark
# para deixar o codigo claro e padronizado.
# spark session.builder inicia a construção de uma sessão do spark.
# o método appName define um nome para nossa aplicação; isso serve para indentificação
# nos logs e em ambientes distribuidos, mas aqui é apenas ilustrativo
# por fim, o método getOrCreate -- cria a sessão se ela ainda não existir
# ou reutiliza uma já existente evitando erros de duplicação ---
spark = SparkSession.builder.appName("AnaliseHousing").getOrCreate()

# aqui usamos a variavel spark para acessar o método read, que é responsavel por ler arquivos
# estamos lendo um arquivo no formato CSV, por isso usamos read.csv
# o parâmetro "housing.csv" é o nome do arquivo, ele precisa estar na mesma pasta que este código
# header=True indica que a primeira linha do arquivo contém os nomes das colunas
# o inferSchema=True faz o spark tentar advinhar automaticamente os tipos (número, texto, etc),
# ao invés de tratar tudo como texto 
df = spark.read.csv("housing.csv", header=True, inferSchema=True)

# Aqui pegamos somente as 5 primeiras linhas do dataframe usando o limit(5)
# df é uma estrutura parecida com uma tabela de banco de dados 
# .toPandas() converte o dataframe do spark em um dataframe do Pandas, 
# que é mais facil de salvar em .csv localmente
# .to_csv() salva o resultado em um novo arquivo chamado "amostra_5_linhas.csv"
# index=false significa que não adicionaremos numeração de linhas no arquivo final
df.limit(5).toPandas().to_csv("amostra_5_linhas.csv", index=False)

# aqui fazemos uma operação de agrupamento usando groupBy
# vamos agrupar os registros pela coluna "ocean_proximity" 
# depois usamos .agg para aplicar funções agregadoras (count e avg)
# count("*") conta quantos registros existem em cada grupo
# avg("median_house_value") calcula a media dos valores medianos de casas por grupo
# alias serve para dar nomes mais claros as colunas do resultado
# convertendo o resultado do pandas para salvar no csv
# este arquivo mostrará quantas casas existem em cada tipo de proximidade
# e o valor médio das casas nesses locais
resultados = df.groupBy("ocean_proximity").agg(
    count("*").alias("total_casas"),
    avg("median_house_value").alias("media_valor_casas")
)

resultados.toPandas().to_csv("resultados_analise.csv", index=False)

# aqui calculamos a media geral de valores das casas no dataset inteiro
# df.agg realiza uma agregação sobre todo o dataframe
# o first() obtém o primeiro valor da primeira linha retornada
# já que o resultado vem em formato estruturado
# aqui contamos quantos registros (linhas) existem no dataset
media_geral = df.agg(avg("median_house_value")).first()[0]
total_registros = df.count()

# agora abrimos um arquivo texto chamado resumo.txt no modo de escrita("w")
# com o 'as f' damos nome ou definimos um nome para alguma coisa
# usamos write para escrever duas linhas: total de registros e media geral dos valores
with open("resumo.txt", "w") as f:
    f.write(f"Total de registros: {total_registros}\n")
    f.write(f"Média geral de valores das casas: {media_geral:.2f}\n")

# esta linha encerra a sessão spark
# é como "desligar o motor" após terminar o trabalho
spark.stop()






#camelCase= estilo de escrita minuscula com maiuscula ex: toPandas lembra corcova de camelo
#snakecase= estilo de escrita com _ ex: maikon_dkf o underline é pra se lembrar de uma cobra
#Pascoalcase= estilo de escrita com inicios maiusculos ex: MaikonSiqueira