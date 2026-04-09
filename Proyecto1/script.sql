-- CREATE DATABASE SGFoodDW;

-- 2. Asegurarse de estar en la BD correcta
USE SGFoodDW;

-- 3. Borrar tablas si existen (para poder re-ejecutar el script limpiamente)
IF OBJECT_ID('FactVentasInventario', 'U') IS NOT NULL DROP TABLE FactVentasInventario;
IF OBJECT_ID('DimProducto', 'U') IS NOT NULL DROP TABLE DimProducto;
IF OBJECT_ID('DimCliente', 'U') IS NOT NULL DROP TABLE DimCliente;
IF OBJECT_ID('DimCanal', 'U') IS NOT NULL DROP TABLE DimCanal;
IF OBJECT_ID('DimFecha', 'U') IS NOT NULL DROP TABLE DimFecha;

-- 4. Creación de Dimensiones
CREATE TABLE DimFecha (
    FechaKey INT PRIMARY KEY, -- Formato YYYYMMDD
    Fecha DATE NOT NULL,
    Anio INT,
    Mes INT,
    NombreMes VARCHAR(20),
    Dia INT,
    DiaSemana VARCHAR(20)
);

CREATE TABLE DimProducto (
    ProductoKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductoSKU VARCHAR(50) NOT NULL,
    NombreProducto VARCHAR(255),
    Marca VARCHAR(100),
    Categoria VARCHAR(100),
    Subcategoria VARCHAR(100),
    Fabricante VARCHAR(100)
);

CREATE TABLE DimCliente (
    ClienteKey INT IDENTITY(1,1) PRIMARY KEY,
    ClienteId VARCHAR(50) NOT NULL,
    NombreCliente VARCHAR(255),
    SegmentoCliente VARCHAR(100),
    Departamento VARCHAR(100),
    Municipio VARCHAR(100)
);

CREATE TABLE DimCanal (
    CanalKey INT IDENTITY(1,1) PRIMARY KEY,
    CanalVenta VARCHAR(100) NOT NULL
);

-- 5. Creación de Tabla de Hechos (Fact Table)
CREATE TABLE FactVentasInventario (
    VentaKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    FechaKey INT NOT NULL,
    ProductoKey INT NOT NULL,
    ClienteKey INT NOT NULL,
    CanalKey INT NOT NULL,
    CantidadVendida INT,
    InventarioInicial INT,
    InventarioFinal INT,
    PrecioUnitario DECIMAL(18,2),
    CostoUnitario DECIMAL(18,2),
    Descuento DECIMAL(18,2),
    ImporteNeto DECIMAL(18,2),
    MargenEstimado DECIMAL(18,2),
    -- Relaciones (Integridad Referencial)
    CONSTRAINT FK_Venta_Fecha FOREIGN KEY (FechaKey) REFERENCES DimFecha(FechaKey),
    CONSTRAINT FK_Venta_Producto FOREIGN KEY (ProductoKey) REFERENCES DimProducto(ProductoKey),
    CONSTRAINT FK_Venta_Cliente FOREIGN KEY (ClienteKey) REFERENCES DimCliente(ClienteKey),
    CONSTRAINT FK_Venta_Canal FOREIGN KEY (CanalKey) REFERENCES DimCanal(CanalKey)
);

-- Índice para búsquedas por Fecha
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FactVentas_Fecha' AND object_id = OBJECT_ID(N'FactVentasInventario'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_FactVentas_Fecha 
    ON FactVentasInventario(FechaKey) 
    INCLUDE (CantidadVendida, ImporteNeto, MargenEstimado);
END;

-- Índice para búsquedas por Producto
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FactVentas_Producto' AND object_id = OBJECT_ID(N'FactVentasInventario'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_FactVentas_Producto 
    ON FactVentasInventario(ProductoKey) 
    INCLUDE (CantidadVendida, ImporteNeto, MargenEstimado);
END;

-- Índice para búsquedas por Cliente
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FactVentas_Cliente' AND object_id = OBJECT_ID(N'FactVentasInventario'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_FactVentas_Cliente 
    ON FactVentasInventario(ClienteKey) 
    INCLUDE (CantidadVendida, ImporteNeto, MargenEstimado);
END;