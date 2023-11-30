import sqlite3
import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns

####Leyendo los archivos    
archivos = [
    "Cierre_agricola_mun_2003.csv",
    "Cierre_agricola_mun_2004.csv",
    "Cierre_agricola_mun_2005.csv",
    "Cierre_agricola_mun_2006.csv",
    "Cierre_agricola_mun_2007.csv",
    "Cierre_agricola_mun_2008.csv",
    "Cierre_agricola_mun_2009.csv",
    "Cierre_agricola_mun_2010.csv",
    "Cierre_agricola_mun_2011.csv",
    "Cierre_agricola_mun_2012.csv",
    "Cierre_agricola_mun_2013.csv",
    "Cierre_agricola_mun_2014.csv",
    "Cierre_agricola_mun_2015.csv",
    "Cierre_agricola_mun_2016.csv",
    "Cierre_agricola_mun_2017.csv",
    "Cierre_agricola_mun_2018.csv",
    "Cierre_agricola_mun_2019.csv",
    "Cierre_agricola_mun_2020.csv",
    "Cierre_agr_mun_2021.csv",
    "Cierre_agr_mun_2022.csv"
]
combinaciones = [
    ('Idestado', 'Nomestado'),
    ('Idddr', 'Nomddr'),
    ('Idcader', 'Nomcader'),
    ('Idmunicipio', 'Nommunicipio'),
    ('Idciclo', 'Nomcicloproductivo'),
    ('Idmodalidad', 'Nommodalidad'),
    ('Idunidadmedida', 'Nomunidad')
]
def crear_tabla_desde_archivo(archivo, conn):
    fecha = archivo.split('_')[3].split('.')[0]
    df = pd.read_csv(f"Datos/{archivo}", encoding='ISO-8859-1')
    nombre_tabla = f"tabla_{fecha}"
    df.to_sql(nombre_tabla, conn, index=False, if_exists='replace')

def limpiar_y_crear_tabla(archivo, conn):
    fecha = archivo.split('_')[3].split('.')[0]
    df = pd.read_csv(f"Datos/{archivo}", encoding='ISO-8859-1')
    columna_precio = "Precio" if "Precio" in df.columns else "Preciomediorural" #Aqui se generaliza las columnas con nombres ditintos 
    columnas = [
        "Anio", "Idestado", "Idddr", "Idcader", "Idmunicipio", "Idciclo",
        "Idmodalidad", "Idunidadmedida", "Idcultivo", "Sembrada", "Cosechada",
        "Siniestrada", "Volumenproduccion", "Rendimiento", columna_precio, "Valorproduccion"
    ]
    df_clean = df[columnas].rename(columns={columna_precio: "Precio"})
    tabla_clean = f"tabla_clean_{fecha}"
    df_clean.to_sql(tabla_clean, conn, index=False, if_exists='replace')


conn = sqlite3.connect('DB_AGRICOLA3.db') #se abre la conexion con la base de datos 

for archivo in archivos:
    if f"tabla_{archivo.split('_')[3].split('.')[0]}" not in pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)["name"].tolist():
        crear_tabla_desde_archivo(archivo, conn)
    limpiar_y_crear_tabla(archivo, conn)

años = range(2003, 2023)


for combinacion in combinaciones:  # se crean las distintas tablas con la informacion mas segmentada 
    nombre_columna_id, nombre_columna_nom = combinacion
    consulta_sql = f'SELECT DISTINCT {nombre_columna_id}, {nombre_columna_nom} FROM tabla_{años[0]}'
    
    for año in años[1:]:
        consulta_sql += f' UNION SELECT DISTINCT {nombre_columna_id}, {nombre_columna_nom} FROM tabla_{año}'
    
    df_resultado = pd.read_sql_query(consulta_sql, conn)
    nombre_tabla = f'{nombre_columna_id}_{nombre_columna_nom}_tabla'
    df_resultado.to_sql(nombre_tabla, conn, index=False, if_exists='replace')
    print(f'Tabla creada: {nombre_tabla}')


años2 = range(2015, 2021)

## Aqui se vuelven a generalizar los nombres de las tablas
consulta2_sql = f'''
    SELECT Idcultivo,
           CASE
               WHEN Anio BETWEEN 2015 AND 2020 THEN "Nomcultivo Sin Um" 
               ELSE "Nomcultivo"
           END AS NombreCultivo
    FROM tabla_{años[0]}
'''

for año in años2[1:]:
    consulta2_sql += f'''
    UNION
    SELECT Idcultivo,
           CASE
               WHEN Anio BETWEEN 2015 AND 2020 THEN "Nomcultivo Sin Um"
               ELSE "Nomcultivo"
           END AS NombreCultivo
    FROM tabla_{año}
'''

#se obtiene un DataFrame apartir de la consulta
df_resultado2 = pd.read_sql_query(consulta2_sql, conn)
nombre_tabla2 = 'Idcultivo_NombreCultivo_tabla'
df_resultado2.to_sql(nombre_tabla2, conn, index=False, if_exists='replace')
print(f'Tabla creada: {nombre_tabla2}')


conn.close()

#####################################################################

conn = sqlite3.connect('DB_AGRICOLA3.db')
años = range(2003, 2023)

consulta_sql3 = f'''
    SELECT Idcultivo,
           Anio,
           SUM(Sembrada) AS SumaSembrada,
           SUM(Cosechada) AS SumaCosechada,
           SUM(Siniestrada) AS SumaSiniestrada,
           SUM(Volumenproduccion) AS SumaVolumenproduccion,
           SUM(Rendimiento) AS SumaRendimiento,
           SUM(Precio) AS SumaPrecio,
           SUM(Valorproduccion) AS SumaValorproduccion
    FROM tabla_clean_{años[0]}
    GROUP BY Idcultivo, Anio
'''

for año in años[1:]:
    consulta_sql3 += f'''
    UNION
    SELECT Idcultivo,
           Anio,
           SUM(Sembrada) AS SumaSembrada,
           SUM(Cosechada) AS SumaCosechada,
           SUM(Siniestrada) AS SumaSiniestrada,
           SUM(Volumenproduccion) AS SumaVolumenproduccion,
           SUM(Rendimiento) AS SumaRendimiento,
           SUM(Precio) AS SumaPrecio,
           SUM(Valorproduccion) AS SumaValorproduccion
    FROM tabla_clean_{año} WHERE Anio= {año} 
    GROUP BY Idcultivo
'''

print(consulta_sql3)
Ventas_por_cultivo_anuales = pd.read_sql_query(consulta_sql3, conn)
print(Ventas_por_cultivo_anuales)



conn.close()

#########se realizan consultas y se grafican algunos de los datos optenidos
output_directory = "C:/Users/ulises/Documents/Reto_Cristian/Imagenes/"
if not os.path.exists(output_directory):
    os.makedirs(output_directory)
for i in años:
    df_plot = Ventas_por_cultivo_anuales[Ventas_por_cultivo_anuales['Anio'] == i]
    df_plot_sorted = df_plot.sort_values(by='SumaValorproduccion', ascending=False)
    df_plot_top10 = df_plot_sorted.head(10)
    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 6))
    sns.barplot(x='Idcultivo', y='SumaValorproduccion', data=df_plot_top10)
    plt.xlabel('Idcultivo')
    plt.ylabel('Valor de produccion')
    plt.title(f'Top 10 Productos con mayor de Valor de produccion en México durante {i}')
    plt.xticks(rotation=45)
    # nombre_archivo = f"Top10_Productos_Mexico_{i}.png"
    # ruta_archivo = os.path.join(output_directory, nombre_archivo)
    # plt.savefig(ruta_archivo)
    plt.show()

#####################################################################################################


conn = sqlite3.connect('DB_AGRICOLA3.db')
años = range(2003, 2023)

consulta_sql4 = f'''
    SELECT Idcultivo,
           Idestado,
           Anio,
           SUM(Sembrada) AS SumaSembrada,
           SUM(Cosechada) AS SumaCosechada,
           SUM(Siniestrada) AS SumaSiniestrada,
           SUM(Volumenproduccion) AS SumaVolumenproduccion,
           SUM(Rendimiento) AS SumaRendimiento,
           SUM(Precio) AS SumaPrecio,
           SUM(Valorproduccion) AS SumaValorproduccion
    FROM tabla_clean_{años[0]} WHERE Idestado=8
    GROUP BY Idcultivo, Anio
'''

for año in años[1:]:
    consulta_sql4 += f'''
    UNION
    SELECT Idcultivo,
           Idestado,
           Anio,
           SUM(Sembrada) AS SumaSembrada,
           SUM(Cosechada) AS SumaCosechada,
           SUM(Siniestrada) AS SumaSiniestrada,
           SUM(Volumenproduccion) AS SumaVolumenproduccion,
           SUM(Rendimiento) AS SumaRendimiento,
           SUM(Precio) AS SumaPrecio,
           SUM(Valorproduccion) AS SumaValorproduccion
    FROM tabla_clean_{año} WHERE Anio= {año} AND Idestado=8
    GROUP BY Idcultivo
'''

print(consulta_sql4)
Ventas_por_cultivo_anuales_estado = pd.read_sql_query(consulta_sql4, conn)
print(Ventas_por_cultivo_anuales_estado)

conn.close()


    


for j in años:
    df_plot2 = Ventas_por_cultivo_anuales_estado[Ventas_por_cultivo_anuales_estado['Anio'] == j]
    df_plot2_sorted = df_plot2.sort_values(by='SumaValorproduccion', ascending=False)
    df_plot2_top10 = df_plot2_sorted.head(10)
    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 6))
    sns.barplot(x='Idcultivo', y='SumaValorproduccion', data=df_plot2_top10)
    plt.xlabel('Idcultivo')
    plt.ylabel('Valor de produccion')
    plt.title(f'Top 10 Productos con mayor de Valor de produccion en Chihuahua durante {j}')
    plt.xticks(rotation=45)
    # nombre_archivo = f"Top10_Productos_Chihuahua_{j}.png"
    # ruta_archivo = os.path.join(output_directory, nombre_archivo)
    # plt.savefig(ruta_archivo)
    plt.show()



#####################################################################################################


conn = sqlite3.connect('DB_AGRICOLA3.db')
años = range(2003, 2023)

consulta_sql5 = f'''
    SELECT Idcultivo,
           Idestado,
           Anio,
           SUM(Sembrada) AS SumaSembrada,
           SUM(Cosechada) AS SumaCosechada,
           SUM(Siniestrada) AS SumaSiniestrada,
           SUM(Volumenproduccion) AS SumaVolumenproduccion,
           SUM(Rendimiento) AS SumaRendimiento,
           SUM(Valorproduccion) AS SumaValorproduccion
    FROM tabla_{años[0]} WHERE Nommunicipio="Juárez"
    GROUP BY Idcultivo, Anio
'''

for año in años[1:]:
    consulta_sql5 += f'''
    UNION
    SELECT Idcultivo,
           Idestado,
           Anio,
           SUM(Sembrada) AS SumaSembrada,
           SUM(Cosechada) AS SumaCosechada,
           SUM(Siniestrada) AS SumaSiniestrada,
           SUM(Volumenproduccion) AS SumaVolumenproduccion,
           SUM(Rendimiento) AS SumaRendimiento,
           SUM(Valorproduccion) AS SumaValorproduccion
    FROM tabla_{año} WHERE Anio= {año} AND Nommunicipio="Juárez"
    GROUP BY Idcultivo
'''

print(consulta_sql5)
Ventas_por_cultivo_anuales_municipio = pd.read_sql_query(consulta_sql5, conn)
print(Ventas_por_cultivo_anuales_municipio)

conn.close()




for k in años:
    df_plot3 = Ventas_por_cultivo_anuales_municipio[Ventas_por_cultivo_anuales_municipio['Anio'] == k]
    df_plot3_sorted = df_plot3.sort_values(by='SumaValorproduccion', ascending=False)
    df_plot3_top10 = df_plot3_sorted.head(10)
    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 6))
    sns.barplot(x='Idcultivo', y='SumaValorproduccion', data=df_plot3_top10)
    plt.xlabel('Idcultivo')
    plt.ylabel('Valor de produccion')
    plt.title(f'Top 10 Productos con mayor de Valor de produccion en Juárez durante {k}')
    plt.xticks(rotation=45)
    # nombre_archivo = f"Top10_Productos_Juarez_{k}.png"
    # ruta_archivo = os.path.join(output_directory, nombre_archivo)
    # plt.savefig(ruta_archivo)
    plt.show()


#####################################################################################################

# conn = sqlite3.connect('DB_AGRICOLA3.db')
# años = range(2003, 2023)

# consulta_sql6 = f'''
#     SELECT Idcultivo,    
#            SUM(Sembrada) AS SumaSembrada,
#            SUM(Cosechada) AS SumaCosechada,
#            SUM(Siniestrada) AS SumaSiniestrada,
#            SUM(Volumenproduccion) AS SumaVolumenproduccion,
#            SUM(Precio) AS SumaPrecio,
#            SUM(Valorproduccion) AS SumaValorproduccion
#     FROM tabla_clean_{años[0]}
#     GROUP BY Idcultivo
# '''

# for año in años[1:]:
#     consulta_sql6 += f'''
#     UNION
#     SELECT Idcultivo,
#            SUM(Sembrada) AS SumaSembrada,
#            SUM(Cosechada) AS SumaCosechada,
#            SUM(Siniestrada) AS SumaSiniestrada,
#            SUM(Volumenproduccion) AS SumaVolumenproduccion,
#            SUM(Precio) AS SumaPrecio,
#            SUM(Valorproduccion) AS SumaValorproduccion
#     FROM tabla_clean_{año}  
#     GROUP BY Idcultivo
# '''

# print(consulta_sql6)
# Ventas_por_cultivo_totales = pd.read_sql_query(consulta_sql6, conn)
# print(Ventas_por_cultivo_totales)
# conn.close()
# conn = sqlite3.connect('DB_AGRICOLA3.db')

# consulta_sql7 = f'''
#     SELECT Idcultivo
#     FROM Idcultivo_NombreCultivo_tabla 
# '''

# print(consulta_sql7)
# Ids_productos = pd.read_sql_query(consulta_sql7, conn)
# print(Ids_productos)

# conn.close()


# lista_de_idcultivo = Ids_productos['Ventas_por_cultivo_totales'].tolist()

# totales=[] 
# for k in lista_de_idcultivo:
    
#     df_plot8 = Ventas_por_cultivo_totales[Ventas_por_cultivo_totales['Idcultivo'] == k]
#     sumaso = sum(df_plot8['SumaValorproduccion']) 
#     totales.append(sumaso)

# totales_ordenados = sorted(totales, reverse=True)

# sns.set(style="whitegrid")
# plt.figure(figsize=(12, 6))
# sns.barplot(x='Idcultivo', y='totales_ordenados')
# plt.xlabel('Idcultivo')
# plt.ylabel('Valor de produccion')
# plt.title(f'Top 10 Productos con mayor de Valor de produccion en Mexico durante {j}')
# plt.xticks(rotation=45)
# plt.show()





   


