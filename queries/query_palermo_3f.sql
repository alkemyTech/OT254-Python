select universidad as university,
	   careers as career,
	   fecha_de_inscripcion as inscription_date,
	   Split_part(p3f.names,'_', 1) as first_name,
	   Split_part(p3f.names,'_', 2) as last_name,
	   sexo as gender,
	   (current_date - birth_dates::date)/365 as age,
	   codigo_postal as postal_code,
	   codigo_postal as location,
	   correos_electronicos as email
	   
from palermo_tres_de_febrero as p3f
where fecha_de_inscripcion::date > '2020/09/01'
	and fecha_de_inscripcion::date < '2021/02/01';