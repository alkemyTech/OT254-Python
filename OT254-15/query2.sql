
-- En este caso cree la tabla localidad para subir los datos del csv con pgadmin
-- (la tabla ya está en la base de datos)
create table localidad(
    codigo_postal integer,
    localidad varchar(100)
);

--segunda query 100% funcional
select
    svm.universidad,
    svm.carrera,
    svm.fecha_de_inscripcion,
    split_part(svm.nombre, '_', 1) as first_name,
    split_part(svm.nombre, '_', 2) as last_name,
    svm.sexo,
    EXTRACT(year FROM AGE(year_19xx(DATE(fecha_nacimiento)))) AS age,
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