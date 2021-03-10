/*
    "Country": "Afghanistan",
    "CountryCode": "AF",
    "Lat": "33.94",
    "Lon": "67.71",
    "Confirmed": 0,
    "Deaths": 0,
    "Recovered": 0,
    "Active": 0,
    "Date": "2020-01-22T00:00:00Z",
*/
/*
[
    {
        "Country": "Mexico",
        "Slug": "mexico",
        "ISO2": "MX"
    },
*/
create table pais
(
	ID 				INT				NOT NULL IDENTITY(1, 1),  	--pk
	nome			varchar(100)     not null,
	slug			varchar(100)     null,
	sigla           	varchar(2)      null,
	CONSTRAINT 		pk_pais 	primary key (id)
);

/*
[{"Country":"Brazil","CountryCode":"BR",
"Province":"","City":"","CityCode":"",
"Lat":"-14.24","Lon":"-51.93",
"Cases":0,"Status":"confirmed","Date":"2020-01-22T00:00:00Z"},
*/
create table dados_paises
(
	ID 				INT				NOT NULL IDENTITY(1, 1),  --pk
	id_pais			int			not null,	 	 --fk pais
	lat			decimal			not null,
	lon			decimal 		not null,
	confirmed		int			null,
	deaths			int			null,
	recovered		int			null,
	active			int			null,
	date			datetime		not null,
	
	constraint 		pk_id			primary key (id),
	constraint fk_dados_paises_pais 		foreign key (id_pais)
		references pais(id)
);

create table log 
(
	ID 				INT				NOT NULL IDENTITY(1, 1), --pk
	data		datetime	not null,
	descricao	text		not null,
	constraint 	pk_log		primary key (id)
);
