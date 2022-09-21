# KPI 06 Disconfort
# KPI PEN (KPIS for a Building Energy Management Platform)
# Copyright (C) <2022>  <TRACASA Instrumental  and UTE i3i/Larraby>
#
# Código fuente para KPI's - Indicadores clave para utilizados por la Plataforma de Gestión Energética de
# Vivienda Social (NASUVINSA) https://www.nasuvinsa.es/
#
# Source Code for KPIs - Key Perfomance Indicators used by a Buliding Energy Management Platform
# for Social Housing in NAVARRA, managed by NASUVINSA https://www.nasuvinsa.es/
#
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU 
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR 
# PURPOSE.  See the GNU General Public License for more details. 
#
# You should have received a copy of the GNU General Public License along with this program.  
# If not, see <https://www.gnu.org/licenses/>.
#
# Contacto TRACASA: eperdomo@itracasa.es
# Contacto i3i/Larraby: oscar@larraby.com ; iradier.aguirre@i3i.es
#
# Código desarrollado por TRACASA Instrumental https://itracasa.es/ en colaboración con la unión de 
# empresas Larraby-I3i http://larraby.com/ - https://www.i3i.es/. 
#
# El desarrollo de este Software para NASUVINSA, ha contado con la ayuda económica y técnica del 
# Proyecto Europeo H2020 STARDUST “Holistic and integrated urban model for Samrt Cities”,
# acuerdo de subvención nº 774094. 
# https://ec.europa.eu/inea/en/horizon-2020/projects/h2020-energy/smart-cities-communities/stardust
#
# Puede distribuirlo y/o modificarlo bajo los términos de la GNU Affero General Public License v3 
# publicada por la Free Software Foundation disponible en https://www.gnu.org/licenses/agpl-3.0.html. 
# Este código se publica con el ánimo de que sea útil pero sin ninguna garantía o responsabilidad de 
# ningún tipo por parte de los autores. Vea la licencia completa para más detalles.
# ---------------------
# KPI 06 Disconfort
# ---------------------
# El confort en la vivienda se tiene cuando la temperatura, la humedad relativa y la calidad del aire se 
# encuentran dentro de los límites estipulados. Se calculan las horas que la vivienda se encuentra fuera
# de los criterios de confort durante los últimos 7 días.
#
# Datos de configuración en kpi.cfg
# Ejemplo:
# [kpi_06]
# temperatura_limite_inferior_verano = 17.0
# temperatura_limite_superior_verano = 26.0
# temperatura_limite_inferior_invierno = 17.0
# temperatura_limite_superior_invierno = 26.0
# humedad_relativa_limite_inferior_verano = 30.0
# humedad_relativa_limite_superior_verano = 62.5
# humedad_relativa_limite_inferior_invierno = 30.0
# humedad_relativa_limite_superior_invierno = 55.0
# nivel_co2_limite_superior = 1200.0
# # 20/3
# DOY_verano = 79
# # 22/9
# DOY_invierno = 265
# entity_vent = .*:vent
#
# Tabla kpi_edi
# -------------
# CREATE TABLE {schema}.kpi_edi (
# 	edificio_id text,
# 	kpi_id text,
# 	fecha text,
# 	v_01 real,
# 	v_02 real,
# 	v_03 real,
# 	"time" timestamp with time zone
# );

#%% Disconfort
import pandas as pd
import sqlalchemy as sa
import numpy as np 
from kpi_functions import clean_kpi_viv, desde, hasta # type: ignore
import configparser

kpi = 'KPI_06'
config = configparser.ConfigParser()
config.read('kpi.cfg')
edificio = config['DEFAULT']['edificio']
schema = config['db']['schema']
crate_engine_read = config['db']['crate_engine_read']
crate_engine_write = config['db']['crate_engine_write']
base_path = config['DEFAULT']['base_path']

# Valores que definen los límites de confort para temperatura, humedad y CO2 en verano e invierno
temperatura_limite_inferior_verano = config.getfloat('kpi_06','temperatura_limite_inferior_verano')
temperatura_limite_superior_verano = config.getfloat('kpi_06','temperatura_limite_superior_verano')
temperatura_limite_inferior_invierno = config.getfloat('kpi_06','temperatura_limite_inferior_invierno')
temperatura_limite_superior_invierno = config.getfloat('kpi_06','temperatura_limite_superior_invierno')
humedad_relativa_limite_inferior_verano = config.getfloat('kpi_06','humedad_relativa_limite_inferior_verano')
humedad_relativa_limite_superior_verano = config.getfloat('kpi_06','humedad_relativa_limite_superior_verano')
humedad_relativa_limite_inferior_invierno = config.getfloat('kpi_06','humedad_relativa_limite_inferior_invierno')
humedad_relativa_limite_superior_invierno = config.getfloat('kpi_06','humedad_relativa_limite_superior_invierno')
nivel_co2_limite_superior = config.getfloat('kpi_06','nivel_co2_limite_superior')
DOY_verano = config.getint('kpi_06','DOY_verano') # 20/3
DOY_invierno = config.getint('kpi_06','DOY_invierno') # 22/9

desde = "2022-08-13"
hasta = "2022-08-20"

entity_vent = config['kpi_06']['entity_vent']

# Temperatura, humedad y co2 medios horarios para cada vivienda el último año a df
sql_query = f"""
            select
                '{edificio}' as edificio_id,
                '{kpi}' as kpi_id,
                substr(entity_id,13,5) as v,
                date_format('%Y-%m-%d %H:%i', date_trunc('hour', 'Europe/Madrid', time_index)) as time,
                avg(temperatura_ambiente) as t,
                avg(humedad_relativa) as h,
                avg(nivel_co2) as c
            from {schema}.etbuilding
            where
                entity_id ~ '{entity_vent}' and
                time_index >= '{desde}' and 
                time_index <= '{hasta}'
            group by time, entity_id
            order by v ASC,time ASC;
            """

df = pd.read_sql(sql_query, crate_engine_read, parse_dates=['time'])

#%% Nuevo campo disconfort en df con 0 ó 1 según criterios en especificaciones de KPI_06
df['DOY']=df['time'].dt.dayofyear
df['estacion']=np.where(((df['DOY'] > 79) & (df['DOY'] < 265)), 'verano', 'invierno')

conditions = [
    (df['estacion']=='verano') & 
    ((df['t'] < temperatura_limite_inferior_verano) | 
    (df['t'] > temperatura_limite_superior_verano) | 
    (df['h'] < humedad_relativa_limite_inferior_verano) | 
    (df['h'] > humedad_relativa_limite_superior_verano) |
    (df['c'] > nivel_co2_limite_superior)),
    (df['estacion']=='invierno') & 
    ((df['t'] < temperatura_limite_inferior_invierno) | 
    (df['t'] > temperatura_limite_superior_invierno) | 
    (df['h'] < humedad_relativa_limite_inferior_invierno) | 
    (df['h'] > humedad_relativa_limite_superior_invierno) |
    (df['c'] > nivel_co2_limite_superior)),
    ]
choices = [1, 1]
df['disconfort'] = np.select(conditions, choices, default=0)

#%% Suma de horas de disconfort por día para cada vivienda en df3
df2 = df[['time','v','disconfort']]
day = pd.Grouper(key='time', freq='D')
df3 = df2.groupby(['v', day]).disconfort.sum().to_frame('v_01').reset_index()
df3['fecha']=df3['time'].dt.strftime('%Y-%m-%d')
df3[['edificio_id','kpi_id']]=[edificio,kpi]

# %% Almacena en DB la suma de horas de disconfort por día para cada vivienda
# sqlalchemy 1.3
engine = sa.create_engine(crate_engine_write)
try:
    clean_kpi_viv(engine, edificio, kpi)
except Exception as error:
    print(Exception)
df3[['edificio_id','kpi_id','v','fecha','v_01','time']].to_sql('kpi_viv',con=engine,schema=schema,if_exists='append',index=False)

