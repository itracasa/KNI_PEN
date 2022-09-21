# KPI 03 Autonomía de silo
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
# KPI 03 Autonomía de silo
# ---------------------
# Cálculo de la disponibilidad de energía almacenada en madera a partir de la capacidad máxima del silo,
# la fecha de recarga completa del silo, capacidad calorífica de la madera utilizada y de los consumos
# energéticos desde la fecha de carga hasta el día actual
#
# Datos de configuración en kpi.cfg
# Ejemplo:
# [kpi_03]
# fecha_recarga = 2022-04-06
# capacidad_silo (Kg) = 9760
# poder_calorifico_astilla = 4.196
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
import numpy as np 
from datetime import datetime
from kpi_functions import clean_kpi, desde, hasta # type: ignore
import datetime
import configparser

kpi='KPI_03'
config = configparser.ConfigParser()
config.read('kpi.cfg')
edificio = config['DEFAULT']['edificio']
schema = config['db']['schema']
crate_engine_read = config['db']['crate_engine_read']
crate_engine_write = config['db']['crate_engine_write']

capacidad_silo = config.getint('kpi_06','capacidad_silo')
poder_calorifico_astilla = config.getfloat('kpi_06','poder_calorifico_astilla')
fecha_recarga = config['kpi_06']['fecha_recarga']

#%% Cálculo de energía total cuando el silo está lleno
energia_inicial = int(capacidad_silo * poder_calorifico_astilla)
print('energía inicial =' + str(energia_inicial))

# Cálulo de consumos energéticos desde fecha de llenado hasta hoy
ent_prod_energia = config['entities']['ent_prod_energia']
ent_termica_primario_1 = config['entities']['ent_termica_primario_1']
ent_termica_primario_2 = config['entities']['ent_termica_primario_2']
query =  f"""
        SELECT
        '{edificio}' as edificio_id,
        '{kpi}' as kpi_id,
        date_format('%Y-%m-%d %H:%i', date_trunc('day', time_index)) as time,
        max({ent_termica_primario_1})-min({ent_termica_primario_1}) as e 
        FROM {schema}.etbuilding 
        where entity_id='{ent_prod_energia}' and time_index >= '{fecha_recarga}'
        group by time 
        order by time
        """
df = pd.read_sql(query, crate_engine_read, parse_dates=['time'])
print(df)

#%% Cálculo de energía teórica disponible
df['fecha'] = df['time'].dt.strftime('%m-%d')
df['consumido'] = df['e'].cumsum()
df['disponible'] = energia_inicial - df['consumido']
df.rename(columns={'disponible':'v_01'},inplace=True)


# %% Carga en tabla de KPIs
# sqlalchemy 1.3
engine = sa.create_engine(crate_engine_write)
try:
    clean_kpi(engine, edificio, kpi)
except Exception as error:
    print(Exception)
df[['edificio_id','kpi_id','fecha','v_01','time']].to_sql('kpi_edi',con=engine,schema=schema,if_exists='append',index=False)
