import streamlit as st
import pandas as pd
from sqlalchemy import create_engine 
from shapely.geometry import Polygon,Point,mapping,shape
import shapely.wkt as wkt
import copy

import folium
import streamlit.components.v1 as components
from streamlit_folium import st_folium
from folium.plugins import Draw
from bs4 import BeautifulSoup

# streamlit run D:\Dropbox\Empresa\CBRE\PROYECTO_BUSQUEDA_INMUEBLES\APP\app.py
# https://streamlit.io/
# pipreqs --encoding utf-8 "D:\Dropbox\Empresa\CBRE\PROYECTO_BUSQUEDA_INMUEBLES\APP"

st.set_page_config(page_icon ="https://www.cbre.com/-/media/project/cbre/dotcom/global/unsorted/favicon_lg.png?rev=f6bed35a1dfb4fac9ede077cef213618")

st.image('https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/cbre/CBRE_Group-Logo.png')