#######################################                                  
# fecha: 14-01-2023                   #      
# ultima modificacion: 14-02-2023     #  
# Gabriel Aillapan                    #                
#######################################

from auxiliares.auxi import sel_num_cols
from validaciones import is_dataframe , df_has_numtype , df_has_null

def sumary(dataframe ):
    """
    Esta función recibe un dataframe de PySpark. 
    La función devuelve un dataframe con un resumen estadistico de todas las
    columnas de tipo numericas, este resumen toma como base a la funcíon Summary()
    de pyspark y le agrega la moda y antimoda al dataframe. En caso de que no haya 
    columnas tipo numericas la función no hara nada y retornara el mismo dataframe entregado.
    
    Argumento:
    dataframe (pyspark.sql.dataframe.DataFrame): Dataframe que desea obtener datos estadisticos.
    
    Retorno:
    dataframe (pyspark.sql.dataframe.DataFrame): Dataframe de PySpark con el resumen estadistico mejorado en caso de tener columnas de tipo numericas.
    """
    is_dataframe(dataframe) 
    if df_has_numtype(dataframe):
        num_cols = sel_num_cols(dataframe)
        df_columns = dataframe.select(*num_cols)
        lista_modas= ['moda']
        lista_antimoda = ['antimoda']
        lista_columnas = ['summary']
        resultado , _  = df_has_null(dataframe)
        mensaje = 'las columnas numericas tienen datos nulos' if resultado else 'columnas sin datos nulos'
        print(mensaje)
        for col in num_cols:
            mode_val = dataframe.groupBy(col).count().sort('count', ascending=False).first()[0]
            anti_mode_val = dataframe.groupBy(col).count().sort('count', ascending=True).first()[0]
            lista_modas.append(mode_val)
            lista_antimoda.append(anti_mode_val)
        for valor in num_cols: lista_columnas.append(valor)
        
        lista_modas = tuple(lista_modas)
        lista_antimoda = tuple(lista_antimoda)
        new_row = spark.createDataFrame([lista_modas] , [*lista_columnas])
        new_row_anti_mode = spark.createDataFrame([lista_antimoda] , [*lista_columnas])
        df_columns.summary().unionAll(new_row).unionAll(new_row_anti_mode).show()
        return df_columns.asummary().unionAll(new_row).unionAll(new_row_anti_mode)
    else : 
        print('El dataframe pyspark propocionado no tiene numeros')
        return dataframe