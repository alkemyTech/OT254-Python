SELECT 
--se modifica el formado de la fecha de nacimiento y se resta con la fecha actual para calcular la edad.
date_part('year', CURRENT_DATE) -  date_part('year', TO_DATE(fecha_nacimiento,'DD-mon-YY'))  AS age,
--se divide en nombre y apellido utilizando el _ como condicional
split_part(nombre, '_', 1) AS first_name,
split_part(nombre, '_', 2) AS last_name,
	--se seleccional el resto de columnas solicitadas.
       universidad, 
	carrera, 
       TO_DATE(fecha_de_inscripcion,'DD-mon-YY') AS inscription_date, 
       sexo, 
       direccion, 
       email,
       localidad

FROM public.salvador_villa_maria
WHERE (universidad = 'UNIVERSIDAD_DEL_SALVADOR') -- se filtran los registros por universidad del salvador.
-- se comprueba si los registros se encuentran entre las fechas requeridas.
AND (TO_DATE(fecha_de_inscripcion,'DD-mon-YY') >= '2020-09-01') AND (TO_DATE(fecha_de_inscripcion,'DD-mon-YY') <= '2021-02-01');
