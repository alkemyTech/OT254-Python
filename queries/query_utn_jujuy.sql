select university,
	   career,
	   inscription_date,
	   Split_part(nombre,' ', 1) as first_name,
	   Split_part(nombre,' ', 2) as last_name,
	   sexo as gender,
	   (current_date - birth_date::date)/365 as age,
	   jujuy_utn.location as postal_code,
	   jujuy_utn.location,
	   email
	   
from jujuy_utn
where inscription_date > '2020/09/01'
	and inscription_date < '2021/02/01';