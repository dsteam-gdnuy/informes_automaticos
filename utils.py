import json
import snowflake.connector
import smtplib
import pandas as pd

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from datetime import date
from config import configuration

def get_credentials(type: str) -> dict:

    f = open('credentials.json',)
    credentials = json.load(f)[type]

    return credentials

def snowflake_login()-> snowflake.connector.connection.SnowflakeConnection:

    credentials = get_credentials('Snowflake')

    USER, PASS, ACCOUNT = credentials['USER'], credentials['PASS'], credentials['ACCOUNT']

    ctx = snowflake.connector.connect(
            user=USER,
            password=PASS,
            account=ACCOUNT,
            database="SANDBOX_PLUS",
            schema="DWH"
            )
    
    cs = ctx.cursor()

    return cs

def process_json():

    # Lee y devuelve sql_informes.json
    f = open('sql_informes.json',)
    data = json.load(f)

    return data

def search_message(message: str) -> str:

    if message == "PAGNIFIQUE_PENETRACION":

        content= """
            Hola, envío informe de % de penetracion de los productos pagnifique de la última semana adjunto,
            
            Saludos.
        """

    elif message == "PAGNIFIQUE_RESULTADOS_PROMO":

        content = """
            Hola, envío informe de promociones adjunto,
            
            Saludos.
        """

    elif message == "PAGNIFIQUE_PENETRACION_MENSUAL":

        content= """
            Hola, envío informe de % de penetracion de los productos pagnifique del último mes adjunto,
            
            Saludos.
        """

    elif message == "COMPARACION_PUNTOS_CLIENTES":

        content= """
            Hola, envío comparación de puntos de clientes e información adicional adjunto,
            
            Saludos.
        """

    elif message == "EVOLUCION_SOCIOS":

        content= """
            Hola, envío evolución de clientes adjunto,
            
            Saludos.
        """
    
    else:
        
        content = """
            Este es un mensaje de prueba
        """    
    
    return content

def create_email(sender_address: str, 
                 receiver_address: str, 
                 sender_pass: str , 
                 message_type: str, 
                 subject: str,
                 df: pd.DataFrame) -> bool:
    
    #Defino mensaje a incluir
    content = search_message(message_type)

    #Instancio mensaje a enviar:
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = subject

    #Armo cuerpo de mail
    message.attach(MIMEText(content, 'plain'))

    #Adjunto el dataframe como csv
    attach_df_as_csv(message, df, filename=f"{message_type}.csv")

    #Llamo funcion para enviar mail
    envio = send_email(message= message, sender_address= sender_address, sender_pass= sender_pass)

    if envio:
        return True
    else:
        return False

def send_email(message: str, sender_address: str, sender_pass: str) -> bool:

    #sesion SMTP
    session = smtplib.SMTP('smtp.gmail.com', 587)
    #Autorizacion seguridad
    session.starttls()
    #Login con credenciales
    session.login(sender_address, sender_pass)
    text = message.as_string()
    #Envio mail
    session.sendmail(sender_address, message["To"].split(","), text)
    #Cierro sesion
    session.quit()
    
    return True

def attach_df_as_csv(msg: MIMEMultipart, df: pd.DataFrame, filename: str) -> None:
    # 1) Convertir DF -> CSV (en memoria)
    csv_str = df.to_csv(index=False)             # agregá sep=';' si lo necesitás
    csv_bytes = csv_str.encode("utf-8")          # a bytes para el payload

    # 2) Armar el adjunto MIME
    part = MIMEBase("text", "csv")
    part.set_payload(csv_bytes)
    encoders.encode_base64(part)                 # seguro para cualquier cliente
    part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
    part.add_header("Content-Type", 'text/csv; charset="utf-8"')

    # 3) Adjuntar al mensaje
    msg.attach(part)