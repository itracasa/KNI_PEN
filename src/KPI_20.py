# KPI_20 - Porcentaje de reducción anual emisiones CO2
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
# KPI_20 - Porcentaje de reducción anual emisiones CO2
# ---------------------
# Diferencia entre las emisiones de  CO2 y las emisiones que se producirían si el edificio siguiera 
# criterios CTE-2019 y solo se usara gas natural.
#
# Datos de configuración en kpi.cfg
# Ejemplo:
# [kpi_20]
# coste_energia = 0.08
# # kgCO2/kWh por	Gas natural
# ve_314 = 0.202
# #	kgCO2/kWh por Electricidad
# ve_312 = 0.31
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

#%%
import pandas as pd
import sqlalchemy as sa
from kpi_functions import clean_kpi, desde, hasta # type: ignore
import datetime
import configparser

config = configparser.ConfigParser()
config.read('kpi.cfg')
edificio = config['DEFAULT']['edificio']
schema = config['db']['schema']
crate_engine_read = config['db']['crate_engine_read']
crate_engine_write = config['db']['crate_engine_write']
base_path = config['DEFAULT']['base_path']
kpi = 'KPI_20'
kpi_path = base_path + '/' + edificio + '/' + kpi + '.json'

coste_energia = config.getfloat('kpi_20','coste_energia')

# Variables estáticas según especificaciones de KPI_20
ve_314 = config.getfloat('kpi_20','ve_314') # kgCO2/kWh por	Gas natural
ve_312 = config.getfloat('kpi_20','ve_312') # kgCO2/kWh por Electricidad

desde = desde()
hasta = hasta()

ent_prod_energia = config['entities']['ent_prod_energia']
ent_termica_primario_1 = config['entities']['ent_termica_primario_1']
ent_termica_primario_2 = config['entities']['ent_termica_primario_2']

# consumo total mensual calculado a partir de diferencia entre mayor y menor valores para cada mes
sql_query = f"""SELECT
            '{edificio}' as edificio_id,
            '{kpi}' as kpi_id,
            date_format('%Y-%m', date_trunc('month', time_index)) as fecha,
            max({ent_termica_primario_1})-min({ent_termica_primario_1}) as primario_1,
            max({ent_termica_primario_2})-min({ent_termica_primario_2}) as primario_2
            FROM {schema}.etbuilding
            where entity_id='{ent_prod_energia}'
            and time_index >= '{desde}'
            and time_index <= '{hasta}'
            group by fecha
            order by fecha asc;"""

df = pd.read_sql(sql_query, crate_engine_read)

# Valores de emisiones estándar según especificaciones de KPI_20
df['emisiones_gas'] = df['primario_2'] * 0.67 * 0.182 # Emisiones totales (en este caso por gas)
df['mes']=df['fecha'].str.slice(5)

coeficiente_consumo = config.getint('edificio','coeficiente_consumo')
superficie = config.getfloat('edificio','superficie')
consumo_mes = { '01':0.185,'02':0.14,'03':0.091,'04':0.062,\
                '05':0.04,'06':0.04,'07':0.04,'08':0.04,\
                '09':0.04,'10':0.04,'11':0.108,'12':0.172 }
df['coef_cons_mes'] = df['mes'].map(consumo_mes)
df['energia_referencia'] = superficie * coeficiente_consumo * df['coef_cons_mes'] # Consumo energético de referencia
df['emisiones_referencia'] = df['energia_referencia'] * 0.202 # Emisiones del edificio de referencia
df['v_01'] = (df['emisiones_referencia']-df['emisiones_gas'])/df['emisiones_referencia']*100 # Ahorro energético respecto edificio de referencia
df['v_02'] = (df['emisiones_referencia']-df['emisiones_gas'])/1000 # Ahorro energético respecto edificio de referencia
df['time']=pd.to_datetime(df['fecha'])


# %% Almacena en DB
# sqlalchemy 1.3
engine = sa.create_engine(crate_engine_write)
clean_kpi(engine, edificio, kpi)
df[['edificio_id','fecha','kpi_id','v_01','v_02','time']].to_sql('kpi_edi',con=engine,schema=schema,if_exists='append',index=False)

#%% Genera fichero con datos en formato JSON para exportaci'on medinte API Rest
df[['edificio_id','kpi_id','fecha','v_01','v_02']].to_json(kpi_path , orient = 'split' , index = False)

