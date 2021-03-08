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

create table dados_paises
(
	id				int 			not null auto_increment, #	pk
	country			varchar(40)		not null,
	countryCode		char(2)			not null,
	lat				decimal			not null,
	lon				decimal 		not null,
	confirmed		int				null,
	deaths			int				null,
	recovered		int				null,
	active			int				null,
	date			datetime		not null,
	
	constraint 		pk_id			primary key (id)
);
