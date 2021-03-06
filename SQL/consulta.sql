-- Panorama di?rio de quantidade de casos confirmados de COVID-19 dos 10 pa?ses do mundo com maiores n?meros.

SELECT TOP 10 PAIS.NOME, SUMARY_PAISES.NEWCONFIRMED
FROM PAIS
INNER JOIN SUMARY_PAISES
ON PAIS.ID = SUMARY_PAISES.ID_PAIS
ORDER BY SUMARY_PAISES.NEWCONFIRMED DESC

-- Panorama di?rio de quantidade de mortes de COVID-19 dos 10 pa?ses do mundo com n?meros.

SELECT TOP 10 PAIS.NOME, SUMARY_PAISES.NEWDEATHS
FROM PAIS
INNER JOIN SUMARY_PAISES
ON PAIS.ID = SUMARY_PAISES.ID_PAIS
ORDER BY SUMARY_PAISES.NEWDEATHS DESC

-- Total de mortes por COVID-19 dos 10 pa?ses do mundo com maiores n?meros.

SELECT TOP 10 PAIS.NOME, SUMARY_PAISES.TOTALDEATHS
FROM PAIS
INNER JOIN SUMARY_PAISES
ON PAIS.ID = SUMARY_PAISES.ID_PAIS
ORDER BY SUMARY_PAISES.TOTALDEATHS DESC

-- Total de casos confirmados por COVID-19 dos 10 pa?ses do mundo com maiores n?meros.

SELECT TOP 10 PAIS.NOME, SUMARY_PAISES.TOTALCONFIRMED
FROM PAIS
INNER JOIN SUMARY_PAISES
ON PAIS.ID = SUMARY_PAISES.ID_PAIS
ORDER BY SUMARY_PAISES.TOTALCONFIRMED DESC
