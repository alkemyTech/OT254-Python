select
    university,
    career,
    inscription_date,
    split_part(nombre, ' ', 1) as first_name,
    split_part(nombre, ' ', 2) as last_name,
    sexo as gender,
    extract(year from age(date(birth_date))) as age, 
	codigo_postal as postal_code,
    location,
    email
from
	jujuy_utn ju,
	localidad l
where
	date(inscription_date) between '2020-09-01' and '2021-01-01'
and
	university = 'universidad tecnológica nacional'
and
	UPPER(location) = UPPER(l.localidad);