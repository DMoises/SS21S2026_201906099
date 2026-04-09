-- ==========================================
-- SCRIPT DE VALIDACIÓN Y ANÁLISIS - PROYECTO 1
-- ==========================================
USE SGFoodDW;

-- 1. AUDITORÍA DE CARGA (Validación de conteos e integridad referencial)
SELECT 'DimProducto' AS Entidad, COUNT(*) AS TotalRegistros FROM DimProducto UNION ALL
SELECT 'DimCliente', COUNT(*) FROM DimCliente UNION ALL
SELECT 'DimCanal', COUNT(*) FROM DimCanal UNION ALL
SELECT 'DimFecha', COUNT(*) FROM DimFecha UNION ALL
SELECT 'FactVentasInventario', COUNT(*) FROM FactVentasInventario;

-- 2. CONSULTA ANALÍTICA COMPLEJA (Comprobación del Modelo Estrella)
-- Objetivo: Identificar el Top 5 de Categorías que generan mayor ganancia, 
-- cruzando la tabla de hechos con la dimensión de productos.
SELECT TOP 5
    p.Categoria,
    SUM(f.CantidadVendida) AS TotalUnidadesVendidas,
    SUM(f.ImporteNeto) AS IngresosTotales,
    SUM(f.MargenEstimado) AS GananciaNeta
FROM FactVentasInventario f
INNER JOIN DimProducto p ON f.ProductoKey = p.ProductoKey
GROUP BY p.Categoria
ORDER BY GananciaNeta DESC;