USE Practica1_SS2;
GO

-- =======================================================
-- 1. ¿Cuál es el Top 5 de destinos más frecuentes?
-- =======================================================
SELECT TOP 5 
    d.Codigo_Aeropuerto AS Destino, 
    COUNT(f.ID_Vuelo) AS Numero_de_Vuelos
FROM Fact_Vuelos f
JOIN Dim_Aeropuerto d ON f.ID_Aeropuerto_Destino = d.ID_Aeropuerto
GROUP BY d.Codigo_Aeropuerto
ORDER BY Numero_de_Vuelos DESC;


-- =======================================================
-- 2. Distribución de vuelos e ingresos por Género del Pasajero
-- =======================================================
SELECT 
    p.Genero, 
    COUNT(f.ID_Vuelo) AS Cantidad_Pasajeros,
    SUM(f.Precio_Boleto_USD) AS Ingresos_Totales_USD
FROM Fact_Vuelos f
JOIN Dim_Pasajero p ON f.ID_Pasajero = p.ID_Pasajero
GROUP BY p.Genero
ORDER BY Cantidad_Pasajeros DESC;


-- =======================================================
-- 3. ¿Qué Aerolíneas tienen el mayor promedio de retraso?
-- =======================================================
SELECT 
    a.Nombre_Aerolinea, 
    AVG(f.Retraso_Minutos) AS Promedio_Retraso_Minutos,
    MAX(f.Retraso_Minutos) AS Retraso_Maximo
FROM Fact_Vuelos f
JOIN Dim_Aerolinea a ON f.ID_Aerolinea = a.ID_Aerolinea
JOIN Dim_Status s ON f.ID_Status = s.ID_Status
WHERE s.Estado_Vuelo = 'DELAYED' -- Filtramos solo los que sí se retrasaron
GROUP BY a.Nombre_Aerolinea
ORDER BY Promedio_Retraso_Minutos DESC;


-- =======================================================
-- 4. ¿Cuántos vuelos y cuánto dinero generamos por Mes?
-- (Demostrando el poder de la Dimensión Tiempo)
-- =======================================================
SELECT 
    t.Anio,
    t.Mes,
    t.Nombre_Mes, 
    COUNT(f.ID_Vuelo) AS Total_Vuelos,
    SUM(f.Precio_Boleto_USD) AS Ingresos_Mensuales
FROM Fact_Vuelos f
JOIN Dim_Tiempo t ON f.ID_Tiempo_Salida = t.ID_Tiempo
GROUP BY t.Anio, t.Mes, t.Nombre_Mes
ORDER BY t.Anio, t.Mes;