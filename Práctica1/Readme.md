# Práctica 1: ETL con Python - De Dataset Crudo a Tabla Relacional Lista para Análisis

**Curso:** Seminario de Sistemas 2  
**Estudiante:** Daniel Moisés Chan Pelico
**Carnet:** 201906099

## 1. Descripción del Proceso ETL

El presente proyecto implementa un flujo ETL (Extract, Transform, Load) desarrollado íntegramente en Python (utilizando `pandas`) para poblar un Data Warehouse en Microsoft SQL Server. 

* **Extracción (Extract):** Se realizó la lectura de un dataset fuente (`dataset_vuelos_crudo.csv`) compuesto por registros transaccionales heterogéneos. Se aplicó manejo de excepciones para garantizar la continuidad del proceso.
* **Transformación (Transform):** Esta fase fue crucial para asegurar la Calidad del Dato. Las operaciones incluyeron:
    * **Estandarización y Homologación:** Se unificaron textos a mayúsculas y se aplicó un diccionario de mapeo para corregir inconsistencias categóricas (ej. unificación de 'M' y 'MASCULINO', 'F' y 'FEMENINO').
    * **Limpieza de Formatos:** Se corrigieron valores numéricos mal formateados (sustitución de comas por puntos en precios).
    * **Tratamiento de Nulos y Fechas:** Se imputaron valores cero en métricas de vuelos cancelados y se filtraron registros sin una fecha de salida válida.
* **Carga (Load):** Se utilizó `SQLAlchemy` y `pyodbc`. Se implementó un enfoque de carga relacional jerárquica: primero se insertaron los registros únicos en las tablas de Dimensiones, se recuperaron las Llaves Subrogadas (Identity Keys) generadas por SQL Server, y finalmente se realizó un `MERGE` iterativo para ensamblar y cargar la Tabla de Hechos.

## 2. Diseño del Modelo Multidimensional

Se diseñó una arquitectura analítica basada en un **Esquema Estrella (Star Schema)**, optimizado para consultas OLAP.

* **Grano del Modelo:** Transaccional (1 fila en la tabla de hechos representa un registro de vuelo único).
* **Tabla de Hechos (`Fact_Vuelos`):** Centraliza las llaves foráneas y almacena métricas aditivas: `Duracion_Minutos`, `Retraso_Minutos`, `Precio_Boleto_USD`, `Equipaje_Total` y `Equipaje_Documentado`.
* **Dimensiones Conformadas:**
    * `Dim_Pasajero`, `Dim_Aerolinea`, `Dim_Aeronave`, `Dim_Detalle_Boleto`, `Dim_Status`.
    * `Dim_Aeropuerto`: Implementada como una dimensión de rol (Role-playing dimension) para modelar simultáneamente el aeropuerto de Origen y de Destino sin duplicar tablas.
    * `Dim_Tiempo`: Dimensión generada algorítmicamente a partir de la fecha de salida, permitiendo análisis jerárquico (Año, Mes, Nombre del Mes, Día).

*(Ver archivo `diagrama_modelo.png` adjunto en este repositorio para el modelo Entidad-Relación físico).*
![Diagrama del Modelo de Datos](master%20-%20Practica1_SS2%20-%20dbo.png)

## 3. Pasos de Ejecución (Entorno Linux)

1.  **Requisitos Previos:**
    * Python 3.10+ en entorno Linux (ej. Pop!_OS / Ubuntu).
    * Motor de Base de Datos: Microsoft SQL Server 2022 (Ejecutado vía Docker).
    * Driver `msodbcsql18` y `unixodbc-dev` instalados en el sistema operativo.
2.  **Instalación de Dependencias:**
    ```bash
    pip3 install pandas sqlalchemy pyodbc
    ```
3.  **Aprovisionamiento de la Base de Datos:**
    Ejecutar el script DDL en SQL Server para crear la base de datos `Practica1_SS2` y sus tablas.
4.  **Ejecución del Pipeline ETL:**
    ```bash
    python3 etl_vuelos.py
    ```

## 4. Resultados Obtenidos (Análisis de Negocio)

Gracias al modelado dimensional, las consultas analíticas se ejecutaron en milisegundos, revelando los siguientes hallazgos de negocio:

**A. Top 5 Destinos Frecuentes:**
El flujo de pasajeros está altamente concentrado en destinos turísticos y hubs principales de Latinoamérica y Europa. Cancún (CUN) lidera con 1,348 llegadas, seguido muy de cerca por Barcelona (BCN) con 1,343 y San Pedro Sula (SAP) con 1,342.

**B. Distribución Demográfica y de Ingresos (Género):**
Gracias a la correcta homologación de datos durante la fase de transformación, el reporte refleja una base limpia de 3 categorías:
* **Masculino:** 9,434 pasajeros (Ingresos: $726,502.14 USD)
* **Femenino:** 9,016 pasajeros (Ingresos: $694,374.66 USD)
* **No Binario:** 747 pasajeros (Ingresos: $57,561.33 USD)

**C. Análisis de Rendimiento Operativo (Retrasos):**
Al filtrar únicamente los vuelos con estado 'DELAYED', se identificó que **Ryanair** es la aerolínea con el mayor promedio de retraso (137 minutos en promedio, con picos máximos de 239 minutos), seguida por LATAM y British Airways (130 minutos promedio). 

**D. Comportamiento Financiero Mensual:**
La `Dim_Tiempo` permitió agrupar los ingresos fácilmente, revelando la estacionalidad del negocio. Durante el año 2024, el mes más fuerte fue **Agosto** con 821 vuelos y $72,074.68 USD en ingresos. En 2025, la tendencia cambió, mostrando un pico operativo en **Noviembre** con 873 vuelos y $68,431.02 USD generados.