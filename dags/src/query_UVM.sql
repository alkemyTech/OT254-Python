SELECT
	universidad AS university,
	carrera AS career,
	TO_CHAR(fecha_de_inscripcion :: DATE, 'YYYY/MM/DD') AS inscription_date,
	SPLIT_PART(nombre, '_', 1) AS first_name,
	SPLIT_PART(nombre, '_', 2) AS last_name,
	sexo AS gender,
	CASE
		WHEN DATE_PART('year', AGE(TO_DATE(TO_CHAR(fecha_nacimiento :: DATE, 'DD/Mon/YY'), 'DD/Mon/YY'))) :: integer <= 15 
		THEN 
		DATE_PART('year', 
		  AGE( 
			  MAKE_DATE( 
				  (19 || SUBSTRING(TO_CHAR(fecha_nacimiento :: DATE, 'DD/Mon/YY'), 8, 2)) :: integer,
				  DATE_PART('month', TO_DATE(fecha_nacimiento, 'DD/Mon/YY')) :: integer, 
				  DATE_PART('day', TO_DATE(fecha_nacimiento, 'DD/Mon/YY')) :: integer))) :: integer
		ELSE 
		DATE_PART('year', AGE(TO_DATE(TO_CHAR(fecha_nacimiento :: DATE, 'DD/Mon/YY'), 'DD/Mon/YY')))
	END AS age,
	localidad AS location,
	email AS email
FROM salvador_villa_maria
WHERE
	universidad = 'UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA' AND
	TO_DATE(TO_CHAR(fecha_de_inscripcion :: DATE, 'YYYY/MM/DD'), 'YYYY/MM/DD') BETWEEN TO_DATE('2020/09/01', 'YYYY/MM/DD') AND TO_DATE('2021/02/01', 'YYYY/MM/DD')