#######################################                                  
# fecha: 14-01-2023                   #      
# ultima modificacion: 14-02-2023     #  
# Gabriel Aillapan                    #                
#######################################

from pyspark.sql.types import *
def sel_num_cols(dataframe): 
    """
    La función sel_num_cols(dataframe) toma un dataframe de pyspark como entrada y devuelve una lista con los nombres de las columnas que son de tipo numérico.
    La función primero define una lista vacía llamada lista_columnas_numericas que se utilizará para almacenar los nombres de las columnas numéricas.
    se definen los tipos numéricos permitidos en una lista llamada tipos_numericos.
    itera a través de las columnas y tipos de datos del dataframe y si el tipo de dato es uno de los tipos numéricos permitidos,
    se agrega el nombre de la columna a la lista lista_columnas_numericas.
    La función devuelve la lista lista_columnas_numericas que contiene los nombres de las columnas numéricas del dataframe.
    """
    lista_columnas_numericas =  []
    tipos_numericos = [LongType().simpleString(), DoubleType().simpleString(), 
        IntegerType().simpleString() , ShortType().simpleString() ,
        FloatType().simpleString() , DecimalType().simpleString()]
    for columnas, dtype in dataframe.dtypes:
        if dtype in tipos_numericos: lista_columnas_numericas.append(columnas)
    
    return lista_columnas_numericas