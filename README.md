# MVP de Engenharia de Dados - O Rio de Janeiro e os Ônibus: Entendendo o Caos!

## ***Nome:*** **Luís Gabriel Nascimento Simas**

## ***Matrícula:*** **4052025000943**
----
## 1. ***Objetivo***
### Comece pelo objetivo do seu trabalho. Antes de iniciar sua busca pelos dados, pense e descreva claramente qual problema deseja resolver com este MVP. Enumere as perguntas que deseja responder.
### É de extrema importância que esta etapa seja feita antes de iniciar qualquer outra etapa.
### Uma vez traçado o objetivo e conhecendo bem qual problema se deseja resolver, quais perguntas se deseja responder, é hora de iniciar a busca pelos dados.
### Não é necessário atingir todos os objetivos desenhados nesta seção. Assim, não remova perguntas as quais não se conseguiu responder. Deixe a documentação do objetivo intacta e faça uma boa discussão do atingimento deste ao final do trabalho (vide Autoavaliação).
## 2. ***Aquisição dos dados***
### Escolha uma base de dados para utilizar em seu MVP de forma que se possa atingir os objetivos traçados na etapa anterior.

### Há inúmeras bases de dados gratuitas disponíveis na web, por exemplo:

- ### [Google Cloud Public Datasets](https://cloud.google.com/datasets)
- ### [Kaggle](https://www.kaggle.com/datasets)
- ### [Portal da Transparência](https://portaldatransparencia.gov.br/)
- ### [IMDB](https://datasets.imdbws.com/)
- ### [Tableau](https://www.tableau.com/learn/articles/free-public-data-sets)
- ### [Stanford Large Network Dataset Collection](https://snap.stanford.edu/data/index.html)
- ### [Yelp Open Dataset](https://www.yelp.ca/dataset)
### Discutiremos sobre bases de dados abertas disponíveis para o MVP no Discord e iremos montar colaborativamente um repositório de possibilidades.

### Caso haja uma licença de uso para o conjunto de dados escolhido, isto deve constar na documentação do MVP.

## 3. ***Coleta***
### Uma vez definido o conjunto de dados, devemos coletar e armazená-los na nuvem.
### É possível que, a partir de sua escolha do conjunto de dados, seja necessária uma etapa de construção de robôs de coleta, e.g. via Web Scraping. Neste caso, atente-se para questões éticas sobre se é possível utilizar os robôs de coleta de informação nos sites escolhidos.
### Caso tenha optado por utilizar um conjunto de dados real da empresa onde trabalha, tenha bastante cuidado com a confidencialidade destes dados e/ou das análises que serão feitas em sequência.

## 4. ***Modelagem***
### Você deve construir um modelo de dados em Esquema Estrela ou Snowflake, como em um Data Warehouse, ou flat por cada conceito, como em um Data Lake.

### Independentemente do modelo, deve ser construído um Catálogo de Dados contendo minimamente uma descrição detalhada dos dados e seus domínios, contendo valores mínimos e máximos esperados para dados numéricos, e possíveis categorias para dados categóricos.

### Este modelo deve também descrever a linhagem dos dados, de onde os mesmos foram baixados e qual técnica foi utilizada para compor o conjunto de dados, caso haja.

## 5. ***Carga***
### Nesta etapa, será feita a carga dos dados para o Data Warehouse/Data Lake. Na avaliação, nesta etapa, será dado valor pela utilização da pipelines de ETL (Extração, Transformação e Carga) na plataforma de dados utilizada. Iremos discutir pipelines de ETL na Plataforma Databricks durante nossos encontros pelo Zoom e no Discord.

### Deve-se documentar os processos de transformação e carga, principalmente os de transformação, e.g. a junção e conciliação de dois conjuntos de dados diferentes.

## 6. ***Análise***
### Vamos dividir a etapa de análise em duas: qualidade de dados e solução do problema.

### 1.  Qualidade de dados

### Deve ser feita uma análise da qualidade para cada atributo do conjunto de dados. Existem problemas no conjunto de dados? Caso haja, como esses problemas podem ser resolvidos para que não afetem as respostas das perguntas que quer solucionar?

### É possível que não se encontre problemas nos conjuntos de dados, em alguns casos há conjuntos de dados curados e já bem tratados antes de serem disponibilizados. Entretanto, mesmo nestes casos, espera-se que seja feita uma análise de valores por atributo e que se demonstre que não se encontrou problemas.

### 2. Solução do problema

### Chegou o momento de se solucionar o problema em questão, definido preliminarmente nos objetivos. Deve-se buscar respostas para as perguntas elencadas. Para cada resposta obtida tecnicamente através da análise dos dados deve haver uma discussão do seu resultado, conectando os números obtidos às respostas ao problema a ser solucionado.

### Ao final, deve haver uma discussão de uma forma geral sobre a solução do problema a partir das discussões de cada resposta.

### Aqui, podem ser utilizadas bibliotecas Python vistas na disciplina Análise Exploratória e Pré-Processamento de Dados, ou as ferramentas vistas na disciplina Visualização de Informação. Entretanto, caso não tenha ainda cursado estas disciplinas, é possível responder as perguntas do objetivo somente através da linguagem SQL, objeto da disciplina de Banco de Dados ou através da linguagem de consulta do banco NoSQL escolhido, objeto da disciplina de Data Warehouse.

## 7. ***Entrega***
### O trabalho é individual.

### Deverá ser disponibilizado todo código construído em um repositório público do GitHub. Se tiver dúvidas sobre como criar um repositório público no GitHub, [consulte](https://docs.github.com/pt/repositories/creating-and-managing-repositories/creating-a-new-repository)

### Algumas tarefas das etapas do trabalho podem ser feitas a partir de componentes visuais da plataforma de nuvem. Desta forma, deve se gerar evidência da execução destes passos através de screenshots ou vídeos.

### Deve se gerar evidência dos resultados das respostas às perguntas que definem o problema do MVP através de screenshots ou vídeos.