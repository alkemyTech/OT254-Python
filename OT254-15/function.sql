--Esta funcion convierte el date en dado caso que este sea mayor al año 2023
--Ejemplo: si el date es 2024-01-12, la funcion devuelve 1944-01-12
--Ejemplo: si el date es 2022-01-12, la funcion devuelve 2022-01-12
CREATE OR REPLACE FUNCTION year_19xx(date) RETURNS date AS $$
DECLARE
    year INTEGER;
BEGIN
    year := EXTRACT(year FROM $1);
    IF year >= 2023 THEN
        RETURN $1 - INTERVAL '100 year';
    ELSE
        RETURN $1;
    END IF;
END;
$$ LANGUAGE plpgsql;
