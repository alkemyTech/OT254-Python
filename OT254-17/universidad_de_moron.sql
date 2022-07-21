SELECT
universidad AS university,
carrerra AS career,
fechaiscripccion AS inscription_date,
Split_part(nombrre,' ', 1) AS first_name,
Split_part(nombrre,' ', 2) AS last_name,
sexo AS gender,
(current_date - to_date(nacimiento,'DD/MM/YYYY'))/365 AS edad,
codgoposstal :: INT AS postal_code,
eemail AS email
FROM moron_nacional_pampa
WHERE 
universidad LIKE 'Universidad de morón' AND
to_date(fechaiscripccion,'DD/MM/YYYY') > '09/1/2020' AND
to_date(fechaiscripccion,'DD/MM/YYYY') < '02/1/2021';