SELECT
    universidad as university,
    careers as career,
    date(fecha_de_inscripcion) as inscription_date,
    SPLIT_PART(names, '_', 1) AS first_name,
    SPLIT_PART(names, '_', 2) AS last_name,
    sexo as gender,
    EXTRACT(year FROM AGE(year_19xx(DATE(birth_dates)))) AS age,
    ptf.codigo_postal as postal_code,
    localidad as location,
    correos_electronicos as email
FROM public.palermo_tres_de_febrero  ptf, localidad l 
WHERE date(fecha_de_inscripcion) BETWEEN '2020-09-01' AND '2021-01-01'
AND universidad = 'universidad_nacional_de_tres_de_febrero'
AND ptf.codigo_postal::integer = l.codigo_postal;
