#######################################                                  
# fecha: 14-01-2023                   #      
# ultima modificacion: 14-02-2023     #  
# Gabriel Aillapan                    #                
#######################################


from auxiliares.auxi import sel_num_cols
from validaciones import is_dataframe
from pyspark.sql.functions import count , sum , avg

def group_count(dataframe, group_cols):
    """
    Esta función recibe un dataframe de PySpar y una lista de columnas a las que se le hara un conteo,
    se hara el calculo de la suma y el avg de las columnas numericas.
    la función devuelve un dataframe con las columnas agregas de conteo, suma y avg de cada cada columna numerica.

    Parameters:
    dataframe (DataFrame): Dataframe de PySpark que se desea imputar
    group_cols (list): Lista de los nombres de las columnas para agrupar y contar
    Returns:
    DataFrame: Dataframe de PySpark con las nuevas columnas de conteo y calculos matematicos.
    """
    try:
        is_dataframe(dataframe)
        lista_columnas_numericas = sel_num_cols(dataframe)
        df_agrupado = dataframe.groupBy(*group_cols).agg(count("*").alias("t_count"))
        lista_expresiones_agregacion = [count("*").alias("t_count")]
        for columnas_numericas in lista_columnas_numericas:
            lista_expresiones_agregacion.append(sum(columnas_numericas).alias('sum_'+columnas_numericas))
            lista_expresiones_agregacion.append(avg(columnas_numericas).alias('avg_'+columnas_numericas))
        df_agrupado = dataframe.groupBy(*group_cols).agg(*lista_expresiones_agregacion)
        df_agrupado.show()
        return df_agrupado
    except Exception as e: 
        print('Ha ocurrido un error al realizar el conteo: ' , e)