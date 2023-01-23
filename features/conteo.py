#######################################
# Imports y asignaciones importantes   
# version: 0.1.8                                     
# fecha: 14-01-2023                         
# ultima modificacion: 23-01-2023
# Gabriel Aillapan                                    
#######################################

from auxiliares.auxi import sel_num_cols
from validaciones import df_has_null
from pyspark.sql.functions import count , sum , avg

#La función count_dataframe recibe como argumentos un dataframe de pyspark y una columna de ese dataframe.
#  La función se encarga de agrupar los datos del dataframe por la columna especificada y calcular la cantidad de veces que se repiten los datos en cada grupo.
#  Además, también calcula la suma y el promedio de las columnas numéricas del dataframe para cada grupo.
def count_dataframe(dataframe , columna):

    boolean , _ = df_has_null(dataframe)
    if boolean: 
        raise TypeError 
        
    df_agrupado = dataframe.groupBy(columna).agg(count(columna).alias("t_count"))
    lista_columnas_numericas = sel_num_cols(dataframe)

    lista_expresiones_agregacion = [count(columna).alias("t_count")]
    for columnas_numericas in lista_columnas_numericas:
        lista_expresiones_agregacion.append(sum(columnas_numericas).alias('sum_'+columnas_numericas))
        lista_expresiones_agregacion.append(avg(columnas_numericas).alias('avg_'+columnas_numericas))
    df_agrupado = dataframe.groupBy(columna).agg(*lista_expresiones_agregacion)

    df_agrupado.show()
    return df_agrupado


#La función comienza por seleccionar las columnas numéricas del DataFrame utilizando la función sel_num_cols.
#  Luego, agrupa los datos del DataFrame utilizando las columnas especificadas en group_cols y cuenta la cantidad de filas en cada grupo.
#La función crea una lista de expresiones de agregación que incluye la cuenta de filas y suma y promedio de cada columna numérica del dataframe.
#  La función utiliza esta lista de expresiones de agregación para agrupar los datos del dataframe según las columnas especificadas en group_cols.
#  Finalmente, muestra el DataFrame agrupado y lo devuelve.

def group_count_dataframe(dataframe, group_cols):
    lista_columnas_numericas = sel_num_cols(dataframe)
    df_agrupado = dataframe.groupBy(*group_cols).agg(count("*").alias("t_count"))
    lista_expresiones_agregacion = [count("*").alias("t_count")]
    for columnas_numericas in lista_columnas_numericas:
        lista_expresiones_agregacion.append(sum(columnas_numericas).alias('sum_'+columnas_numericas))
        lista_expresiones_agregacion.append(avg(columnas_numericas).alias('avg_'+columnas_numericas))
    df_agrupado = dataframe.groupBy(*group_cols).agg(*lista_expresiones_agregacion)
    df_agrupado.show()
    return df_agrupado