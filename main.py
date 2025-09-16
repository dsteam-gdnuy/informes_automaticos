from informes import type_of_report
import argparse
from datetime import date, timedelta

"""

Este programa va a ser el encargado de descargar los datos solicitados para informes, luego se enviara automaticamente a las casillas de mail
que sean necesarias, se le van a enviar dos argumentos a esta funcion, la fecha con el formato
'YYYY-MM-DD' para ejecutarse en el período solicitado (FECHA - 6 días) y el tipo de reporte para saber cual reporte
ejecutar

"""

if __name__ == '__main__':

    #Instancio fecha default
    current_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    #Creo objeto parser
    parser = argparse.ArgumentParser()

    #Creo argumento fecha
    parser.add_argument('--fecha', type=str, default= current_date)
    parser.add_argument('--tipo', type=str)
    args = parser.parse_args()

    #Llamo función
    type_of_report(args.fecha, args.tipo)
