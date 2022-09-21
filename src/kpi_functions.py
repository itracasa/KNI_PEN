# KPI PEN (KPIS for a Buliding Energy Management Platform)
# Copyright (C) <2022>  <TRACASA Instrumental  and UTE i3i/Larraby>

# Código fuente para KPI's - Indicadores clave para utilizados por la Plataforma de Gestión Energética de
# Vivienda Social (NASUVINSA) https://www.nasuvinsa.es/

# Source Code for KPIs - Key Perfomance Indicators used by a Buliding Energy Management Platform
# for Social Housing in NAVARRA, managed by NASUVINSA https://www.nasuvinsa.es/

# This program is free software: you can redistribute it and/or modify it under the terms of the GNU 
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR 
# PURPOSE.  See the GNU General Public License for more details. 

# You should have received a copy of the GNU General Public License along with this program.  
# If not, see <https://www.gnu.org/licenses/>.

# Contacto TRACASA: eperdomo@itracasa.es
# Contacto i3i/Larraby: oscar@larraby.com ; iradier.aguirre@i3i.es

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
#
# KPI_Functions
#

import sqlalchemy as sa
import datetime
import configparser


def clean_kpi(engine, edificio, kpi):
    """ Limpia en la base de datos el contenedor del KPI seleccionado

    Args:
        engine (crate): Engine SQLAlchemy(CrateDB)
        edificio (text): ID del Edificio
        kpi (text): ID del KPI

    Returns:

    """

    try:
        con = engine.connect()
        trans = con.begin()
        con.execute("DELETE FROM mt" + edificio + ".kpi_edi WHERE edificio_id='" + edificio + "' AND kpi_id='" + kpi + "';")
        trans.commit()
        con.close()
    except Exception as error:
        print(error)

def clean_kpi_viv(engine, edificio, kpi):
    """ Limpia en la base de datos el contenedor del KPI seleccionado

    Args:
        engine (crate): Engine SQLAlchemy(CrateDB)
        edificio (text): ID del Edificio
        kpi (text): ID del KPI

    Returns:

    """

    try:
        con = engine.connect()
        trans = con.begin()
        con.execute("DELETE FROM mt" + edificio + ".kpi_viv WHERE edificio_id='" + edificio + "' AND kpi_id='" + kpi + "'")
        trans.commit()
        con.close()
    except Exception as error:
        print("Error: limpiar_kpi ", error)

def desde():
    return format(datetime.datetime.today().replace(day=1) - datetime.timedelta(days = 366), '%Y-%m-%d')

def hasta():
    return format(datetime.datetime.today().replace(day=1) - datetime.timedelta(days = 1), '%Y-%m-%d')
