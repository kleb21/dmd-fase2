import pandas as pd
from sqlalchemy import create_engine
import pyodbc

# Conexión a SQL Server con autenticación de Windows
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=CALEB\MYSQL_SSIS;'
    'DATABASE=GestionFacturas2;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

# Cargar las hojas de los archivos de Excel
df_supervisor = pd.read_excel(r'C:\Users\cesar\Actividad-Final-DMD\Maestras.xlsx', sheet_name='Supervisor')
df_vendedor = pd.read_excel(r'C:\Users\cesar\Actividad-Final-DMD\Maestras.xlsx', sheet_name='Vendedor')
df_gaseosa = pd.read_excel(r'C:\Users\cesar\Actividad-Final-DMD\Maestras.xlsx', sheet_name='Gaseosa')
df_sector = pd.read_excel(r'C:\Users\cesar\Actividad-Final-DMD\Maestras.xlsx', sheet_name='Sector')
df_pais = pd.read_excel(r'C:\Users\cesar\Actividad-Final-DMD\Países.xlsx', sheet_name='País')
df_facturas = pd.read_excel(r'C:\Users\cesar\Actividad-Final-DMD\Facturas.xlsx', sheet_name='Factura')

# Insertar en la tabla Supervisor
for index, row in df_supervisor.iterrows():
    cursor.execute(
        "SELECT COUNT(*) FROM Supervisor WHERE CodigoSupervisor = ?",
        int(row['Código Supervisor']) 
    )
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            "INSERT INTO Supervisor (CodigoSupervisor, NombreSupervisor) VALUES (?, ?)",
            int(row['Código Supervisor']), str(row['Nombre Supervisor']) 
        )
conn.commit()

# Insertar en la tabla Vendedor
for index, row in df_vendedor.iterrows():
    cursor.execute(
        "SELECT COUNT(*) FROM Vendedor WHERE CodigoVendedor = ?",
        int(row['Código Vendedor']) 
    )
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            "INSERT INTO Vendedor (CodigoVendedor, NombreVendedor, CodigoSupervisor) VALUES (?, ?, ?)",
            int(row['Código Vendedor']), str(row['Nombre Vendedor']), int(row['Código Supervisor']) 
        )
conn.commit()

# Insertar en la tabla Gaseosa
for index, row in df_gaseosa.iterrows():
    cursor.execute(
        "SELECT COUNT(*) FROM Gaseosa WHERE CodigoGaseosa = ?",
        row['Código Gaseosa']
    )
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            "INSERT INTO Gaseosa (CodigoGaseosa, NombreGaseosa, Precio) VALUES (?, ?, ?)",
            row['Código Gaseosa'], row['Nombre Gaseosa'], row['Precio']
        )
conn.commit()

# Insertar en la tabla Sector
for index, row in df_sector.iterrows():
    cursor.execute(
        "SELECT COUNT(*) FROM Sector WHERE CodigoSector = ?",
        row['Código Sector']
    )
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            "INSERT INTO Sector (CodigoSector, DescripcionSector) VALUES (?, ?)",
            row['Código Sector'], row['Descripción Sector']
        )
conn.commit()

# Insertar en la tabla País
for index, row in df_pais.iterrows():
    cursor.execute(
        "SELECT COUNT(*) FROM Pais WHERE CodigoPais = ?",
        row['Código País']
    )
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            "INSERT INTO Pais (CodigoPais, Nombre) VALUES (?, ?)",
            row['Código País'], row['Nombre']
        )
conn.commit()

# Crear e insertar en la tabla NumeroFactura
for index, row in df_facturas[['Número', 'Serie']].drop_duplicates().iterrows():
    cursor.execute(
        "INSERT INTO NumeroFactura (Numero, Serie) VALUES (?, ?)",
        int(row['Número']), str(row['Serie']) 
    )
conn.commit()

# Limitar el data frame para 100
df_facturas_limited = df_facturas.head(100)

# array para hacer un bulk insert
factura_data = []

for index, row in df_facturas_limited.iterrows():
    cursor.execute(
        "SELECT Id FROM NumeroFactura WHERE Numero = ? AND Serie = ?",
        row['Número'], row['Serie']
    )
    id_numero = cursor.fetchone()[0]
    factura_data.append((
        id_numero, row['Fecha'], row['Cantidad'], row['Código Gaseosa'], row['Código País'],
        row['Código Sector'], row['Código Vendedor'], row['IGV'], row['Merma'], row['Ppto'], row['Total']
    ))

# Bulk insert en Factura
cursor.executemany(
    "INSERT INTO Factura (IdNumero, Fecha, Cantidad, CodigoGaseosa, CodigoPais, CodigoSector, CodigoVendedor, IGV, Merma, Ppto, Total) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    factura_data
)

conn.commit()
