1 - Qual o objetivo do comando cache em Spark?
R - O objetivo é permitir que os arquivos que estão sendo trabalhados sejam armazenados em memória, com isso facilitando o acesso aos dados ou arquivos com mais rapidez e facilidade já que estão disponíveis e gravados em cash.


2 - O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?
R - O MapReduce utiliza o Disco Rígido para armazenamento das suas ações, como por ex: o Hive.
O Spark utiliza a memória do cluster para seus processamentos, lembrando que o spark é escalável sendo assim é possível programar o mapReduce de acordo com a necessidade.


Dessa forma a velocidade de leitura e gravação do Spark fica totalmente mais eficaz.
3 - Qual é a função do SparkContext?
R - Ser uma interface entre o spark e a aplicação em execução, é um objeto que permite a utilização dos recursos do spark dentro da aplicação, como funções, ação de transformação e operações.

4 - Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
R - RDD's São os objetos responsáveis por manipular os dados dentro de uma aplicação spark
De forma consistente e "resiliente" tolerantes a falhas, distribuída podendo estar particionadas em diferentes nós do cluster.

5 - GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
R - O Groupbykey tansmite mais dados durante seus processos de mapping, o que pode consumir memória excessiva
O Reducebykey, o dado é agrupado/combinado em uma chave única dentro de cada uma das partições de memória e então é distribuído, reduzindo a quantidade de dados trafegados

6 - Explique o que o código Scala abaixo faz.

val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
.map(word => (word, 1))
.reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")

R -  O RDD TextFile recebe os dados buscados no arquivo em disco através do caminho "Path" no caso HDFS.
É feito uma contagem total de palavras, identificando o Delimitador de cada palavra por um espaço.
É aplicada a função Map que atribui a quantidade de 1 (uma) unidade para cada palavra contabilizada na contagem.
É aplicada a função Reduce para agregar as palavras iguais e somar suas quantidades em um único par (key-value).
O resultado é gravado em um arquivo de texto e salvo no diretório "Path" no caso HDFS.
