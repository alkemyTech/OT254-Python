SELECT
	universidad AS university,
	carrera AS career,
	TO_CHAR(fecha_de_inscripcion :: DATE, 'YYYY/MM/DD') AS inscription_date,
	SPLIT_PART(name, ' ', 1) AS first_name,
	SPLIT_PART(name, ' ', 2) AS last_name,
	sexo AS gender,
	CASE
		WHEN DATE_PART('year', AGE(TO_DATE(TO_CHAR(fecha_nacimiento :: DATE, 'YYYY/MM/DD'), 'YYYY/MM/DD'))) :: integer <= 15 
		THEN 
		DATE_PART('year', 
		  AGE( 
			  MAKE_DATE( 
				  (19 || SUBSTRING(TO_CHAR(fecha_nacimiento :: DATE, 'YYYY/MM/DD'), 3, 2)) :: integer,
				  DATE_PART('month', TO_DATE(fecha_nacimiento, 'YYYY/MM/DD')) :: integer, 
				  DATE_PART('day', TO_DATE(fecha_nacimiento, 'YYYY/MM/DD')) :: integer))) :: integer
		ELSE 
		DATE_PART('year', AGE(TO_DATE(TO_CHAR(fecha_nacimiento :: DATE, 'YYYY/MM/DD'), 'YYYY/MM/DD')))
	END AS age,
	codigo_postal AS postal_code,
	correo_electronico AS email
FROM flores_comahue
WHERE
	universidad = 'UNIVERSIDAD DE FLORES' AND
	TO_DATE(TO_CHAR(fecha_de_inscripcion :: DATE, 'YYYY/MM/DD'), 'YYYY/MM/DD') BETWEEN TO_DATE('2020/09/01', 'YYYY/MM/DD') AND TO_DATE('2021/02/01', 'YYYY/MM/DD');