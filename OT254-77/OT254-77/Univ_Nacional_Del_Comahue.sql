SELECT 
--se modifica el formado de la fecha de nacimiento y se resta con la fecha actual para calcular la edad.
date_part('year', CURRENT_DATE) -  date_part('year', TO_DATE(fecha_nacimiento,'YYYY-MM-DD')) AS age,
--se divide en nombre y apellido utilizando el _ como condicional
split_part(name, ' ', 1) AS first_name,
split_part(name, ' ', 2) AS last_name,
	
       universidad, 
	carrera, 
       fecha_de_inscripcion, 
       sexo, 
       codigo_postal, 
       direccion, 
       correo_electronico 

FROM public.flores_comahue
WHERE (universidad = 'UNIV. NACIONAL DEL COMAHUE')  -- se filtran los registros por universidad del comahue.
-- se comprueba si los registros se encuentran entre las fechas requeridas.
AND (fecha_de_inscripcion >= '2020-09-01') AND (fecha_de_inscripcion <= '2021-02-01');
