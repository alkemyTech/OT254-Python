select
    fc.universidad,
    fc.carrera,
    fc.fecha_de_inscripcion,
    split_part(fc.name, ' ', 1) as first_name,
    split_part(fc.name, ' ', 2) as last_name,
    fc.sexo,
    extract(year from age(date(fc.fecha_nacimiento))) as age,
    fc.codigo_postal,
    fc.direccion,
    fc.correo_electronico
from public.flores_comahue fc
where fecha_de_inscripcion between '2020-09-01' and '2021-01-01'
and fc.universidad = 'UNIVERSIDAD DE FLORES';
