SELECT
    universidad,
    careers,
    fecha_de_inscripcion,
    SPLIT_PART(names, '_', 1) AS first_name,
    SPLIT_PART(names, '_', 2) AS last_name,
    sexo,
    EXTRACT(year FROM AGE(year_19xx(DATE(birth_dates)))) AS age,
    codigo_postal,
    direcciones,
    correos_electronicos
FROM public.palermo_tres_de_febrero  ptf
WHERE date(fecha_de_inscripcion) BETWEEN '2020-09-01' AND '2021-01-01'
AND universidad = 'universidad_nacional_de_tres_de_febrero';
