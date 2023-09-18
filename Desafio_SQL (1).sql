-- Databricks notebook source
drop database desafio_teste cascade;

CREATE DATABASE desafio_teste;
use desafio_teste;

-- COMMAND ----------

-- Criação da tabela com os seus devidos tipos de dados

CREATE table table_titulo(
   title_id int ,
   title varchar(100));
   

CREATE table table_genero(
  genre_id int, 
  genre varchar(50)) ;
  

CREATE table table_usuario(
  user_id int,   
  genre_id int, 
  age int,
  gender varchar(50),
  occupation varchar(50), 
  zip_code varchar(50),
  marital_status varchar(50));


CREATE table table_imdb(
   star_rating int, 
   title_id int, 
   genre_id  int,
   duration int);

-- COMMAND ----------

-- Criação de tabelas stage

CREATE TABLE stage_titulos(
   title_id char(50),
   title  char(100));
   

CREATE TABLE stage_generos(
  genre_id  char(50), 
  genre  char(50));


CREATE TABLE stage_usuarios(
  user_id char(50),   
  genre_id char(50), 
  age char(50),
  gender char(50),
  occupation char(100), 
  zip_code char(50),
  marital_status char(100));


CREATE TABLE stageimdb(
   star_rating char(50), 
   title_id char(50), 
   genre_id  char(50),
   duration char(50));

-- COMMAND ----------

-- Copiando os arquivos para dentro da Stage(decidi utilizar o stage pois estava com problemas para a integração do arquivo csv com o tipos de dados)

copy  INTO stage_titulos
from  '/FileStore/tables/title_dataset-8.csv'
FILEFORMAT = csv 
FORMAT_OPTIONS ('mergeSchema' = 'False',
                'delimiter' = ';',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');


copy  INTO stage_generos
from  '/FileStore/tables/genre_dataset-4.csv'
FILEFORMAT = csv 
FORMAT_OPTIONS ('mergeSchema' = 'False',
                'delimiter' = ';',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');


copy  INTO stage_usuarios
from  '/FileStore/tables/user_dataset-8.csv'
FILEFORMAT = csv 
FORMAT_OPTIONS ('mergeSchema' = 'False',
                'delimiter' = ';',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');



copy  INTO stageimdb
from   '/FileStore/tables/imdb_dataset-5.csv'
FILEFORMAT = csv 
FORMAT_OPTIONS ('mergeSchema' = 'False',
                'delimiter' = ';',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- Substituindo os dados para a Tabela

INSERT INTO table_titulo
select  CAST(title_id as int) as title_id, title from stage_titulos;


INSERT INTO table_genero
select  CAST(genre_id as int) as genre_id, genre from stage_generos;


INSERT INTO table_imdb
select  CAST(star_rating as int) as star_rating,
        CAST(title_id as int) as title_id,
        CAST(genre_id as int) as genre_id,
        CAST(duration as int) as duration  from stageimdb;
        

INSERT INTO table_usuario
select  CAST(user_id as int) as user_id,
        CAST(genre_id as int) as genre_id,
        CAST(age as int) as age,
        CAST(gender as varchar(20)) as gender,
        CAST(occupation as varchar(50)) as occupation,
        CAST(zip_code as char(20)) as zip_code,
        CAST(marital_status as varchar(100)) as marital_status from stage_usuarios;


-- COMMAND ----------

CREATE VIEW rel_imdb as
SELECT 
  CASE
    WHEN star_rating <= 7.8 THEN "Ruim"
    WHEN star_rating > 7.8 AND star_rating <= 8.8 then "Regular"
    WHEN star_rating > 8.8 THEN "Bom"
   end as Status,
  
  CASE
    WHEN duration <= 70 THEN "Curta Duração"
    WHEN duration >70 AND duration <= 120 then "Média Duração"
    WHEN duration > 120 THEN "Longa Duração"
  end as Duracao, * from table_imdb;
  
  
  

-- COMMAND ----------

SELECT * FROM rel_imdb;

-- COMMAND ----------

CREATE VIEW rel_usu as
SELECT 
CASE 
		WHEN gender = 'M' THEN 'Male'
		else 'Female'

END AS Genero, *  FROM table_usuario;

-- COMMAND ----------

SELECT * FROM rel_usu

-- COMMAND ----------

CREATE VIEW relatorio_filme AS 
  SELECT  
          current_date() as data_atual,
          i.Status as status,
          i.duration,
          i.Duracao as duracao,
          i.star_rating as nota, 
          i.title_id,
          i.genre_id,
          u.genero as genero_usu,
          u.user_id,
          u.age as idade,
          u.occupation as profissao,
          u.zip_code as cep,
          u.marital_status as estado_civil,
          g.genre as genero_filme,
          t.title
      FROM rel_imdb as i
      INNER JOIN table_titulo  t 
      ON t.title_id = i.title_id 
      INNER JOIN table_genero AS g 
      ON g.genre_id = i.genre_id
      INNER JOIN rel_usu AS u
      ON u.genre_id = i.genre_id;

-- COMMAND ----------

select * from relatorio_filme

-- COMMAND ----------

-- Quantos filmes estão cadastrados por genero?
-- Quantos filmes possuem duração menor que 120 minutos?
-- Quantos filmes de Ação estão com notas acima de 8.8 ?
-- Qual o Ranking de filmes de ação de acordo com as avaliações?
-- Qual é o genero mais assistidos por Estudandes?
-- Qual é o genero mais assistidos por usuários Divorciados?
-- Quantas mulheres casadas gostam de filmes de Comédia?
-- Quais são os generos menos assistidos?
-- Qual é o genero favorito dos homens? Qual é o genero favorito das mulheres?
-- Quantos usuários, solteiros e acima dos 40 anos gostam de filmes de Fantasia? Quais são os titulos de fantasia disponiveis?
-- Finalizado o desafio, apresente a sua opinião sobre a questão a seguir.

-- O que é melhor trabalhar com todos os dados desnormalizados ou trabalhar com os dados em uma tabela unica?*/

-- COMMAND ----------

select * from relatorio_filme;

-- COMMAND ----------

 -- 1- Quantos filmes estão cadastrados por genero? OK

SELECT 
    genero_filme AS GeneroFilme, 
    count(distinct(title)) AS TotalGeneros 
FROM relatorio_filme
GROUP BY genero_filme
ORDER BY TotalGeneros DESC;

-- Resolução: Fiz uma listagem onde mostra o genero que tem mais filmes cadastrados até o menor;


-- COMMAND ----------

-- 2- Quantos filmes possuem duração menor que 120 minutos? OK

SELECT 
  count(distinct(title)) as duracao_120
from relatorio_filme
WHERE duration < 120;

-- Resolução: 525 filmes possuem duração menor de <120 minutos;

-- COMMAND ----------

-- 3- Quantos filmes de Ação estão com notas acima de 8.8 ? OK

SELECT 
  COUNT(distinct(title)) AS NotasAcima88,
  title AS TituloFilme,
  genero_filme AS GeneroFilme,
  nota as Nota
FROM relatorio_filme
WHERE genero_filme = 'Action' AND nota > 8.8
GROUP BY title, genero_filme, nota;

-- Resolução: Apenas o filme "The Dark Knight" possui nota acima de 8.8 e é de ação;

-- COMMAND ----------

-- 4- Qual o Ranking de filmes de ação de acordo com as avaliações? OK

SELECT 
  distinct(title) AS TituloFilme,
  genero_filme as GeneroFilme,
  nota AS Nota,
  status as Avaliacao
FROM relatorio_filme
WHERE genero_filme = 'Action'
ORDER BY nota desc;

-- Resolução: Fiz um Ranking dos filmes de ação de acordo com as avaliações, o filme mais bem avaliado(ação) é o The Dark Knight.

-- COMMAND ----------

-- 5- Qual é o genero mais assistidos por Estudandes? OK

SELECT 
    DISTINCT(genero_filme), 
    count(genero_filme) AS qtda,
    profissao AS profissao
FROM relatorio_filme
WHERE profissao ='student'
GROUP BY genero_filme, genero_usu, profissao
ORDER BY qtda DESC

-- Resolução: Fiz uma listagem onde mostra todos os generos em ordem do mais assistido mais o menos assistido por estudantes, onde se destaca o genero DRAMA;

-- COMMAND ----------

-- 6- Qual é o genero mais assistidos por usuários Divorciados? OK

SELECT  
    genero_filme AS GeneroFilme,
    estado_civil AS EstadoCivil,
    count(genero_filme) as qtda
FROM relatorio_filme
where estado_civil = 'Divorced'
group by genero_filme, estado_civil
order by qtda DESC
limit 1;

-- Resolução: O genero mais assistido por usuários DIVORCIADOS é de no total de 5415;

-- COMMAND ----------

-- 7- Quantas mulheres casadas gostam de filmes de Comédia? OK

SELECT 
    genero_usu AS Genero,
    estado_civil AS EstadoCivil,
    genero_filme AS GeneroFilme,
    count(title) AS qtda
FROM relatorio_filme
WHERE (genero_usu = 'Female' AND estado_civil = 'Married')
AND genero_filme = 'Comedy'
GROUP BY genero_usu, genero_filme, estado_civil


-- Resolução:  Total de 664 MULHERES , CASADAS gostam de filmes de comédia;

-- COMMAND ----------

SELECT distinct(genero_filme) FROM  relatorio_filme

-- COMMAND ----------

-- 8- Quais são os generos menos assistidos? OK

SELECT 
    genero_filme AS GeneroFilme,
    count(title) AS TotalAssistido 
FROM relatorio_filme
GROUP BY genero_filme
ORDER BY count(title) ASC
limit 5;

-- Resolução: Fiz uma listagem com o top 5 generos menos assistidos;

-- COMMAND ----------

-- 9- Qual é o genero favorito dos homens? Qual é o genero favorito das mulheres? 
SELECT 
    COUNT(title) AS qtd_Assistida,
	count(genero_filme) FILTER(where (genero_usu ='Male')) as Valores_Masc,
	count(genero_filme) FILTER(WHERE(genero_usu = 'Female')) as Valores_Fem,
    genero_filme AS GeneroFilme
FROM relatorio_filme
GROUP BY genero_filme
ORDER BY qtd_Assistida DESC

-- Resolução: Fiz esta resolução de duas maneira, onde a primeira(esta resolução) mostra todos os valores máximos e
-- minimos assistidos em ORDEM de maior quantidade, e seus respectiveis generos tanto de mulheres e homens;


-- COMMAND ----------

-- 9- Qual é o genero favorito dos homens? Qual é o genero favorito das mulheres?
SELECT
MAX(genero) AS genero,
MAX(total_generos) AS Total,
MAX(filme)
FROM (SELECT 
    DISTINCT(genero_usu) AS genero,
    count(title) AS total_generos,
    genero_filme as filme
FROM relatorio_filme
GROUP BY genero_filme, genero_usu, filme
) relatorio_filme
GROUP BY genero, filme
HAVING total = 13110 OR total = 3135
ORDER BY Total DESC


-- Resolução: o valor total de filmes mais assistidos Masculino é de 13110 no genero DRAMA e o
-- o valor total de filmes mais assistidos por Mulheres é 3135 e o genero é DRAMA;

-- COMMAND ----------

/* 10- Quantos usuários, solteiros e acima dos 40 anos gostam de filmes de Fantasia? Quais são os titulos de fantasia disponiveis?*/

SELECT 
  estado_civil AS EstadoCivil,
  genero_filme AS GeneroFilme,
  count(title) AS Total
FROM  relatorio_filme
WHERE (estado_civil = 'Single' AND idade >40) AND genero_filme = 'Fantasy'
group by estado_civil,  genero_filme


-- Resolução: Total de 5 usuários solteiros acima de 40 anos gostam de filme de fantasia.

-- COMMAND ----------

--Finalizado o desafio, apresente a sua opinião sobre a questão a seguir.
-- O que é melhor trabalhar com todos os dados desnormalizados ou trabalhar com os dados em uma tabela unica?*


Trabalhar com mais de uma tabela é desnecessario alem de dar mais trabalho. A utilização de recursos aumentariam valores para consultas, como
utilização de vários joins e dados inutilizados para esse recurso. Então relacionar apenas uma tabela e trabalhar em cima dela é bem mais economico para o banco
e pratico para realização do ETL.
