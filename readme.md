# A Bike Manufacturing Company

Projeto de infraestrutura local, utilizando conceitos de armazenamento organizado em camadas. 

Para as análises iniciais dos datasets, bem como extração/carregamento/tratamento dos dados, foi utilizada a biblioteca Pandas do Python. As queries das análises finais foram feitas com o PySpark SQL.

## O projeto
---

Este projeto foi desenvolvido no intuito de simular a extração, carregamento e tratamento de dados de uma empresa fabricante de bicicletas. Ao final do processo, serão realizadas _queries_ de análises dos dados baseadas em algum objetivo de negócio.

Os diretórios que fazem parte deste projeto são:

- _[data-analysis](https://github.com/rbsmotta/bike-manufacturing-company/tree/main/data-analysis)_, contendo os notebooks das análises prévias estruturais e qualitativas dos datasets originais (_landing_);
- _[elt](https://github.com/rbsmotta/bike-manufacturing-company/tree/main/elt)_, contendo script de _extração, carregamento e transformação_ (main.py) e notebook com as análises finais (queries.ipynb);
- _[bucket](https://github.com/rbsmotta/bike-manufacturing-company/tree/main/bucket)_, contendo todos os datasets do projeto, divido em zonas/camadas;
- _[img](https://github.com/rbsmotta/bike-manufacturing-company/tree/main/img)_, contendo imagens ilustrativas do projeto e diagramas;
- _[references](https://github.com/rbsmotta/bike-manufacturing-company/tree/main/reference)_, contendo referencias de sites e bibliografia.

Para este projeto, foi utilizado um conceito de zona (camadas ou _layers_) de armazenamento dos dados. Cada camada tem um grau de preparação dos dados, desde seu modo mais bruto - denominada zona **_Landing_** (ou _raw layer_) até um grau maior de processamento para análises de negócio - a zona **_Gold_** (ou _curated layer_), passando por uma camada de primeiro contato com engenharia/ciência de dados, na zona denominada **_Work_** (ou _refined layer_).

É importante frisar que o conceito independe do nome dado às camadas. O importante é a abordagem do conceito propriamente dito.

[![Alt text](https://github.com/rbsmotta/bike-manufacturing-company/blob/main/img/diagram.png)]([https://digitalocean.com](https://github.com/rbsmotta/bike-manufacturing-company/blob/main/img/diagram.png))

## A "_Landing Zone_" (ou camada "_Raw_")
---

Os dados nessa camada geralmente são armazenados sem nenhum ou quase nenhum tratamento, deixando-os bem próximos ao seu estado original. 

Para este projeto, foram disponibilizados 6 datasets - todos no formato CSV (_comma separeted values_). Os datasetes são:

- _Person.Person.csv_
- _Production.Product.csv_
- _Sales.Customer.csv_
- _Sales.SalesOrderDetail.csv_
- _Sales.SalesOrderHeader.csv_
- _Sales.SpecialOfferProduct.csv_

Primeiramente, foram feitas análises _estruturais_ e _qualitativas_ nos datasets, visando a identificação de possíveis anomalias e suas correções necessárias para que os datasets possam "avançar" para a próxima camada (_work_ /_curated_). 

Feitas as análises, constataram-se alguns problemas, majoritariamente campos vazios e tipos numéricos em colunas não quantitativas. 

Dependendo da coluna e de sua importância para possíveis análises futuras, foram adicionadas informações no lugar dos espaços vazios ("undefined"), outras colunas foram excluídas por completo. Em alguns casos optamos por excluir as linhas com os dados faltantes. 

Toda essa governança geralmente segue as regras do negócio. Aqui, o foco foi nas análises que foram determinadas e em possíveis análises futuras (no caso hipotético dos dados serem preenchidos em extrações futuras).

Toda as análises podem ser encontradas na pasta __data-analysis__. 

## A "_Work Zone_" (ou camada "_Refined_")
---

É aqui que os datasets resultantes dos tratamentos são salvos. Em nosso projeto, utilizamos o formato de arquivos _parquet_. É um formato de maior eficiência e menor custo de armazenamento e computacional, além de ser o formato ideal para arquivos de datasets que serão objeto de análise. 

É nesse estágio que ocorrem divisões com base no negócio, assuntos, projetos ou de qualquer forma que melhor organize o armazenamento.
Neste projeto, organizamos em três locais distintos:

- _Person_, destinado ao dataset com dados dos clientes;
- _Production_, destinado ao dataset com dados dos produtos;
- _Sales_, destinado aos datasets com dados de vendas.

Nessa camada, não foram feitas modificações nos datasets, sendo feitas apenas as análises de negócio.

## A "_Gold Zone_" (ou camada "_Curated_")
---

É a camada em que serão armazenados os dados processados, no caso, os resultados das análises feitas nos dataset da _Work Zone_. Aqui, os datasets são muitas vezes menores (tanto em numero de linhas como em número de colunas), mais específico para _business analysis_.

Neste projeto, é [aqui](https://github.com/rbsmotta/bike-manufacturing-company/blob/main/elt/queries.ipynb) que se encontram os resultados das seguintes análises solicitadas:

_1- Quantidade de linhas na tabela "Sales.SalesOrderDetail" pelo campo SalesOrderID, desde que tenham pelo menos três linhas_;

_2- Ligando as tabelas "Sales.SalesOrderDetail", "Sales.SpecialOfferProduct" e "Production.Product", retornar o nome dos 3 produtos mais vendidos, pela soma "OrderQty", agrupados pelo número de dias para manufatura (DaysToManufacture)_;

_3- Obter lista de nomes de clientes e uma contagem de pedidos efetuados, utilizando as tabelas "Person.Person", "Sales.Customer" e "Sales.SalesOrderHeader"_;

_4- Obter a soma total de produtos (OrderQty) por ProductID e OrderDate das tabelas "Sales.SalesOrderHeader", "Sales.SalesOrderDetail" e "Production.Product"_;

_5- Obter apenas as linhas onde a ordem tenha sido feita durante o mês de setembro/2011 e o total devido esteja acima de 1000, ordenado pelo total devido descrescente, utilizando os campos SalesOrderID, OrderDate e TotalDue da tabela Sales.SalesOrderHeader_.
