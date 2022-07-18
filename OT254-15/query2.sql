
-- En este caso cree la tabla localidad para subir los datos del csv con pgadmin
-- (la tabla ya está en la base de datos)
create table localidad(
    codigo_postal integer,
    localidad varchar(100)
);

--segunda query funcional 99%
select
    svm.universidad,
    svm.carrera,
    svm.fecha_de_inscripcion,
    split_part(svm.nombre, '_', 1) as first_name,
    split_part(svm.nombre, '_', 2) as last_name,
    svm.sexo,
    -- no funciona correctamente date(svm.fecha_nacimiento)
    -- caso funcional (22-May-87) -> 1987-05-22
    -- caso no funcional (25-Aug-61) -> 2061-08-25, correctamente (1961-08-25)
    extract(year from age(date(svm.fecha_nacimiento))) as age, -- no me funcionó en todos los casos
    l.codigo_postal,
    svm.direccion,
    svm.email
from
	public.salvador_villa_maria svm,
	public.localidad l
where
	date(svm.fecha_de_inscripcion) between '2020-09-01' and '2021-01-01'
and
	svm.universidad = 'UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA'
and
	l.localidad = svm.localidad;
