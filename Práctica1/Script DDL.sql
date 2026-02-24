CREATE DATABASE Practica1_SS2;
GO

USE Practica1_SS2;
GO

-- ==========================================
-- CREACIÓN DE DIMENSIONES
-- ==========================================

-- Dimensión Pasajero
CREATE TABLE Dim_Pasajero (
    ID_Pasajero INT IDENTITY(1,1) PRIMARY KEY, -- Llave Subrogada
    Pasajero_ID_Original VARCHAR(50),          -- Llave Natural (de origen del CSV)
    Genero VARCHAR(10),
    Edad INT,
    Nacionalidad VARCHAR(10)
);

-- Dimensión Aerolínea
CREATE TABLE Dim_Aerolinea (
    ID_Aerolinea INT IDENTITY(1,1) PRIMARY KEY,
    Codigo_Aerolinea VARCHAR(10),
    Nombre_Aerolinea VARCHAR(100)
);

-- Dimensión Aeropuerto (Sirve para Origen y Destino)
CREATE TABLE Dim_Aeropuerto (
    ID_Aeropuerto INT IDENTITY(1,1) PRIMARY KEY,
    Codigo_Aeropuerto VARCHAR(10)
);

-- Dimensión Aeronave
CREATE TABLE Dim_Aeronave (
    ID_Aeronave INT IDENTITY(1,1) PRIMARY KEY,
    Tipo_Aeronave VARCHAR(50)
);

-- Dimensión Detalle de Boleto (Canal, Clase, Pago)
CREATE TABLE Dim_Detalle_Boleto (
    ID_Detalle_Boleto INT IDENTITY(1,1) PRIMARY KEY,
    Clase_Cabina VARCHAR(50),
    Canal_Venta VARCHAR(50),
    Metodo_Pago VARCHAR(50)
);

-- Dimensión Estado del Vuelo
CREATE TABLE Dim_Status (
    ID_Status INT IDENTITY(1,1) PRIMARY KEY,
    Estado_Vuelo VARCHAR(50)
);

-- Dimensión Tiempo (Conformada)
CREATE TABLE Dim_Tiempo (
    ID_Tiempo INT PRIMARY KEY, -- YYYYMMDD
    Fecha DATE,
    Anio INT,
    Mes INT,
    Nombre_Mes VARCHAR(20),
    Dia INT,
    Trimestre INT,
    Dia_Semana VARCHAR(20)
);

-- ==========================================
-- CREACIÓN DE LA TABLA DE HECHOS
-- ==========================================

CREATE TABLE Fact_Vuelos (
    ID_Vuelo INT IDENTITY(1,1) PRIMARY KEY,
    Record_ID_Original INT, -- Para trazabilidad
    
    -- Llaves Foráneas (FK) a las Dimensiones
    ID_Tiempo_Salida INT FOREIGN KEY REFERENCES Dim_Tiempo(ID_Tiempo),
    ID_Aerolinea INT FOREIGN KEY REFERENCES Dim_Aerolinea(ID_Aerolinea),
    ID_Aeropuerto_Origen INT FOREIGN KEY REFERENCES Dim_Aeropuerto(ID_Aeropuerto),
    ID_Aeropuerto_Destino INT FOREIGN KEY REFERENCES Dim_Aeropuerto(ID_Aeropuerto),
    ID_Pasajero INT FOREIGN KEY REFERENCES Dim_Pasajero(ID_Pasajero),
    ID_Aeronave INT FOREIGN KEY REFERENCES Dim_Aeronave(ID_Aeronave),
    ID_Detalle_Boleto INT FOREIGN KEY REFERENCES Dim_Detalle_Boleto(ID_Detalle_Boleto),
    ID_Status INT FOREIGN KEY REFERENCES Dim_Status(ID_Status),
    
    -- Métricas (Hechos numéricos)
    Duracion_Minutos INT,
    Retraso_Minutos INT,
    Precio_Boleto_USD DECIMAL(10,2), -- Usa el estimado en USD para estandarizar
    Equipaje_Total INT,
    Equipaje_Documentado INT
);
GO