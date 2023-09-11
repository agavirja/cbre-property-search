import streamlit as st
import pandas as pd
import re
import json
import requests
from sqlalchemy import create_engine 
from shapely.geometry import Polygon,Point,mapping,shape
import copy


from scripts.formato_direccion import formato_direccion



#-----------------------------------------------------------------------------#
# DATA BOGOTA
#-----------------------------------------------------------------------------#
@st.experimental_memo
def getdatacapital(polygon):
    user     = st.secrets["user_bigdata"]
    password = st.secrets["password_bigdata"]
    host     = st.secrets["host_bigdata"]
    schema   = st.secrets["schema_bigdata"]
    
    engine     = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    datapoints = pd.read_sql_query(f"SELECT lotcodigo FROM  bigdata.data_bogota_lotes_point WHERE ST_CONTAINS(ST_GEOMFROMTEXT('{polygon}'), POINT(longitud, latitud))" , engine)
    query      = '(lotcodigo="'+'" OR lotcodigo="'.join(datapoints['lotcodigo'].unique())+'")'
    dataimport = pd.read_sql_query(f"SELECT lotcodigo as barmanpre, ST_AsText(geometry) as wkt FROM  bigdata.data_bogota_lotes WHERE {query}" , engine)
    query      = '(barmanpre="'+'" OR barmanpre="'.join(datapoints['lotcodigo'].unique())+'")'
    
    # Remover vias
    datacatastro = pd.read_sql_query(f"SELECT  barmanpre  FROM  bigdata.data_bogota_catastro WHERE precdestin IN ('65','66') AND {query}" , engine)
    idd          = dataimport['barmanpre'].isin(datacatastro['barmanpre'])
    if sum(idd)>0:
        dataimport = dataimport[~idd]
    
    # Data catastro
    datacatastro = pd.read_sql_query(f"SELECT id,precbarrio,prenbarrio,prechip,predirecc,preaterre,preaconst,precdestin,precuso,preuvivien,preusoph,prevetustz,barmanpre,latitud,longitud,coddir,piso,estrato  FROM  bigdata.data_bogota_catastro WHERE (precdestin<>'65') AND {query}" , engine)
    dataprecuso,dataprecdestin = getuso_destino()
    dataprecuso.rename(columns={'codigo':'precuso','tipo':'usosuelo','descripcion':'desc_usosuelo'},inplace=True)
    dataprecdestin.rename(columns={'codigo':'precdestin','tipo':'actividad','descripcion':'desc_actividad'},inplace=True)
    datacatastro = datacatastro.merge(dataprecuso,on='precuso',how='left',validate='m:1')
    datacatastro = datacatastro.merge(dataprecdestin,on='precdestin',how='left',validate='m:1')
    datacatastro['formato_direccion'] = datacatastro['predirecc'].apply(lambda x: formato_direccion(x))
    for i in ['preaconst','preaterre']:
        idd = datacatastro[i].isnull()
        if sum(idd)>0:
            datacatastro.loc[idd,i] = 0
    datagrupada = groupcatastro(datacatastro)
    dataimport  = dataimport.merge(datagrupada,on='barmanpre',how='left',validate='m:1')
    
    # Data shd
    datashd      = getdatacapital_sdh(list(datacatastro['prechip'].unique()))
    datashdmerge = datashd.copy()
    datashdmerge = datashdmerge[datashdmerge['valorAutoavaluo']>0]
    datashdmerge = datashdmerge.sort_values(by=['chip','vigencia','valorAutoavaluo'],ascending=False)
    datashdmerge = datashdmerge.groupby('chip').agg({'valorAutoavaluo':'first','valorImpuesto':'first'}).reset_index()
    datashdmerge.columns = ['prechip','avaluocatastral','predial']
    datacatastro = datacatastro.merge(datashdmerge,on='prechip',how='left',validate='m:1')
    datacatastro['avaluoxmt2']  = datacatastro['avaluocatastral']/datacatastro['preaconst']
    datacatastro['predialxmt2'] = datacatastro['predial']/datacatastro['preaconst']

    
    st.session_state.datalotes                 = copy.deepcopy(dataimport)
    st.session_state.datalotes.index           = range(len(st.session_state.datalotes))
    st.session_state.datalotes_origen          = copy.deepcopy(dataimport)
    st.session_state.datalotes_origen.index    = range(len(st.session_state.datalotes_origen))
    st.session_state.datacatastro              = copy.deepcopy(datacatastro)
    st.session_state.datacatastro.index        = range(len(st.session_state.datacatastro))
    st.session_state.datacatastro_origen       = copy.deepcopy(datacatastro)
    st.session_state.datacatastro_origen.index = range(len(st.session_state.datacatastro_origen))
    st.session_state.datashd                   = copy.deepcopy(datashd)
    st.session_state.datashd.index             = range(len(st.session_state.datashd))
    st.session_state.datashd_origen            = copy.deepcopy(datashd)
    st.session_state.datashd_origen.index      = range(len(st.session_state.datashd_origen))
    st.session_state.zoom_start    = 16
    st.session_state.secion_filtro = True
    
    # Data market
    datamarket_venta    = pd.read_sql_query(f"SELECT id,direccion,available,	tipoinmueble,	areaconstruida,	valorventa,	valorarriendo,	latitud,	longitud,	inmobiliaria,	imagen_principal FROM  cbre.data_market_venta_dpto_11 WHERE ST_CONTAINS(ST_GEOMFROMTEXT('{polygon}'), geometry)" , engine)
    datamarket_arriendo = pd.read_sql_query(f"SELECT id,direccion,available,	tipoinmueble,	areaconstruida,	valorventa,	valorarriendo,	latitud,	longitud,	inmobiliaria,	imagen_principal FROM  cbre.data_market_arriendo_dpto_11 WHERE ST_CONTAINS(ST_GEOMFROMTEXT('{polygon}'), geometry)" , engine)
    
    datamarket_venta['tiponegocio']    = 'Venta'
    datamarket_arriendo['tiponegocio'] = 'Arriendo'
    
    st.session_state.datamarket       = pd.concat([datamarket_venta,datamarket_arriendo])
    st.session_state.datamarket.index = range(len(st.session_state.datamarket))
    engine.dispose()
    st.experimental_rerun()
    
@st.experimental_memo
def groupcatastro(df):
    df = df.groupby(['barmanpre']).agg({'formato_direccion':'first','barmanpre':'count','prenbarrio':'first','prevetustz':['min','max'],'estrato':'median','preaconst':'sum','preaterre':'sum','usosuelo': lambda x: list(x.unique()),'actividad':lambda x: list(x.unique())}).reset_index()
    df.columns = ['barmanpre','direccion','predios','barrio','antiguedad_min','antiguedad_max','estrato','areaconstruida','areaterreno','usosuelo','actividad']
    return df

@st.experimental_memo
def getuso_destino():
    user     = st.secrets["user_bigdata"]
    password = st.secrets["password_bigdata"]
    host     = st.secrets["host_bigdata"]
    schema   = st.secrets["schema_bigdata"]
    
    engine         = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    dataprecuso    = pd.read_sql_query("SELECT * FROM  bigdata.bogota_catastro_precuso" , engine)
    dataprecdestin = pd.read_sql_query("SELECT * FROM  bigdata.bogota_catastro_precdestin" , engine)
    engine.dispose()
    return dataprecuso,dataprecdestin

@st.experimental_memo
def getinfopredioscapital(barmanpre):
    
    user     = st.secrets["user_bigdata"]
    password = st.secrets["password_bigdata"]
    host     = st.secrets["host_bigdata"]
    schema   = st.secrets["schema_bigdata"]
    
    engine       = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    datalotes    = pd.read_sql_query(f"SELECT lotcodigo as barmanpre, ST_AsText(geometry) as wkt FROM  bigdata.data_bogota_lotes WHERE lotcodigo='{barmanpre}'" , engine)
    datacatastro = pd.read_sql_query(f"SELECT  id,precbarrio,prenbarrio,prechip,predirecc,preaterre,preaconst,precdestin,precuso,preuvivien,preusoph,prevetustz,barmanpre,latitud,longitud,coddir,piso,estrato  FROM  bigdata.data_bogota_catastro WHERE barmanpre='{barmanpre}'" , engine)
    
    
    dataprecuso,dataprecdestin = getuso_destino()
    dataprecuso.rename(columns={'codigo':'precuso','tipo':'usosuelo','descripcion':'desc_usosuelo'},inplace=True)
    dataprecdestin.rename(columns={'codigo':'precdestin','tipo':'actividad','descripcion':'desc_actividad'},inplace=True)
    datacatastro = datacatastro.merge(dataprecuso,on='precuso',how='left',validate='m:1')
    datacatastro = datacatastro.merge(dataprecdestin,on='precdestin',how='left',validate='m:1')
    datacatastro['formato_direccion'] = datacatastro['predirecc'].apply(lambda x: formato_direccion(x))
    for i in ['preaconst','preaterre']:
        idd = datacatastro[i].isnull()
        if sum(idd)>0:
            datacatastro.loc[idd,i] = 0
            
    datashd         = getdatacapital_sdh(list(datacatastro[datacatastro['prechip'].notnull()]['prechip'].unique()))
    datainfopredios =  getdatainfopredio(list(datacatastro[datacatastro['prechip'].notnull()]['prechip'].unique()))
    if datashd.empty is False:
        datashdmerge = datashd.copy()
        datashdmerge = datashdmerge[datashdmerge['valorAutoavaluo']>0]
        datashdmerge = datashdmerge.sort_values(by=['chip','vigencia','valorAutoavaluo'],ascending=False)
        datashdmerge = datashdmerge.groupby('chip').agg({'valorAutoavaluo':'first','valorImpuesto':'first'}).reset_index()
        datashdmerge.columns = ['prechip','avaluocatastral','predial']
        datacatastro = datacatastro.merge(datashdmerge,on='prechip',how='left',validate='m:1')
        datacatastro['avaluoxmt2']  = datacatastro['avaluocatastral']/datacatastro['preaconst']
        datacatastro['predialxmt2'] = datacatastro['predial']/datacatastro['preaconst']
        datacatastro['rangoarea']   = pd.cut(datacatastro['preaconst'],bins=[0,100,200,300,500,800,1000,float('inf')],labels =['menor a 100 mt2','100 a 200 mt2','200 a 300 mt2','300 a 500 mt2','500 a 800 mt2','800 a 1,000 mt2','mayor a 1,000 mt2'])
        
        datashd  = datashd[['chip','vigencia','direccionPredio','nroIdentificacion','valorAutoavaluo','valorImpuesto','indPago','idSoporteTributario']]
        searchby = datashd[datashd['nroIdentificacion'].notnull()]
        if searchby.empty is False:
            dataowner = getdataowner(list(searchby['nroIdentificacion'].unique()))
        if dataowner.empty is False:
            datashd   = datashd.merge(dataowner,on='nroIdentificacion',how='left',validate='m:1')
    
    
    datagrupada = groupcatastro(datacatastro)
    datalotes   = datalotes.merge(datagrupada,on='barmanpre',how='left',validate='m:1')
    engine.dispose()
    return datalotes,datacatastro,datashd,datainfopredios


@st.experimental_memo
def getdatacapital_sdh(chip):
    user     = st.secrets["user_bigdata"]
    password = st.secrets["password_bigdata"]
    host     = st.secrets["host_bigdata"]
    schema   = st.secrets["schema_bigdata"]
    
    query = ''
    if isinstance(chip, list):
        query = '(chip="'+'" OR chip="'.join(chip)+'")'
    elif isinstance(chip, str):
        query =  'chip="{chip}"'

    datashd = pd.DataFrame()
    if query!='':
        engine   = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
        datashd  = pd.read_sql_query(f"SELECT * FROM bigdata.data_bogota_catastro_vigencia WHERE {query}" , engine)
        engine.dispose()
        
    return datashd

@st.experimental_memo
def getdatainfopredio(chip):
    user     = st.secrets["user_bigdata"]
    password = st.secrets["password_bigdata"]
    host     = st.secrets["host_bigdata"]
    schema   = st.secrets["schema_bigdata"]
    
    query = ''
    if isinstance(chip, list):
        query = '(numeroChip="'+'" OR numeroChip="'.join(chip)+'")'
    elif isinstance(chip, str):
        query =  'numeroChip="{chip}"'

    datainfopredio = pd.DataFrame()
    if query!='':
        engine         = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
        datainfopredio = pd.read_sql_query(f"SELECT * FROM bigdata.data_bogota_catastro_predio WHERE {query}" , engine)
        engine.dispose()
    return datainfopredio

def getparam(x,tipo,pos):
    try: return json.loads(x)[pos][tipo]
    except: return None
    
@st.experimental_memo
def getdataowner(identificacion):
    user     = st.secrets["user_bigdata"]
    password = st.secrets["password_bigdata"]
    host     = st.secrets["host_bigdata"]
    schema   = st.secrets["schema_bigdata"]
    
    query = ''
    if isinstance(identificacion, list):
        query = '(nroIdentificacion="'+'" OR nroIdentificacion="'.join(identificacion)+'")'
    elif isinstance(identificacion, str):
        query =  'nroIdentificacion="{identificacion}"'

    dataowner = pd.DataFrame()
    if query!='':
        engine    = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
        dataowner = pd.read_sql_query(f"SELECT * FROM bigdata.data_bogota_catastro_propietario WHERE {query}" , engine)
        engine.dispose()

        for i in [1,2,3,4,5]:
            dataowner[f'telefono{i}'] = dataowner['telefonos'].apply(lambda x: getparam(x,'numero',i-1))
        for i in [1,2,3]:
            dataowner[f'email{i}'] = dataowner['email'].apply(lambda x: getparam(x,'direccion',i-1))
        for i in [1,2,3]:
            dataowner[f'direccion_contacto{i}'] = dataowner['dirContacto'].apply(lambda x: getparam(x,'direccion',i-1))
        dataowner.drop(columns=['telefonos','email','dirContacto','dirContactoNot','aplicaDescuento','naturaleza'],inplace=True)
    return dataowner


@st.experimental_memo
def tipoinmuebl2PrecUso():
    formato = {
        'Apartamento':['001','002','037','038'],
        'Bodega':['001','008','009','010','012','014','019','025','028','032','033','037','044','053','066','080','081','091','093','097',],
        'Casa':['001','037'],
        'Local':['003','004','006','007','008','009','010','012','019','025','028','039','040','041','042','044','056','057','060','080','081','091','093','094','095'],
        'Oficina':['005','006','015','018','020','041','045','080','081','082','092','094','095','096'],
        'Parqueadero':['005','024','048','049','050','096'],
        'Consultorio':['015','017','020','043','045','092'],
        'Edificio':['024','050'],
        'Hotel':['021','026','027','046'],
        'Lote':['090','000'],
        }
    return formato


# "https://oficinavirtual.shd.gov.co/barcode/certificacion?idSoporte={i['idSoporteTributario']}"





#-----------------------------------------------------------------------------#
# DATA DANE
#-----------------------------------------------------------------------------#
@st.experimental_memo
def censodane(polygon):
    
    # https://geoportal.dane.gov.co/geovisores/territorio/analisis-cnpv-2018/
    coordenadas = re.findall(r"(-?\d+\.\d+) (-?\d+\.\d+)", polygon)
    coordenadas = coordenadas[:-1]
    coordenadas = ",".join([f"{lon},{lat}" for lon, lat in coordenadas])
    url = f"https://geoportal.dane.gov.co/laboratorio/serviciosjson/poblacion/20221215-indicadordatospoligonos.php?coordendas={coordenadas}"
    r   = requests.get(url).json()
    df  = pd.DataFrame(r)
    df.rename(columns={'V1': 'Total viviendas', 'V2': 'Uso mixto', 'V3': 'Unidad no residencial', 'V4': 'Lugar especial de alojamiento - LEA', 'V5': 'Industria (uso mixto)', 'V6': 'Comercio (uso mixto)', 'V7': 'Servicios (uso mixto)', 'V8': 'Agropecuario, agroindustrial, foresta (uso mixto)', 'V9': 'Sin información (uso mixto)', 'V10': 'Industria (uso no residencial)', 'V11': 'Comercio (uso no residencial)', 'V12': 'Servicios (uso no residencial)', 'V13': 'Agropecuario, Agroindustrial, Foresta (uso no residencial)', 'V14': 'Institucional (uso no residencial)', 'V15': 'Lote (Unidad sin construcción)', 'V16': 'Parque/ Zona Verde (uso no residencial)', 'V17': 'Minero-Energético (uso no residencial)', 'V18': 'Protección/ Conservación ambiental (uso no residencial)', 'V19': 'En Construcción (uso no residencial)', 'V20': 'Sin información (uso no residencial)', 'V21': 'Viviendas', 'V22': 'Casa', 'V23': 'Apartamento', 'V24': 'Tipo cuarto', 'V25': 'Vivienda tradicional indígena', 'V26': 'Vivienda tradicional étnica (Afrocolombiana, Isleña, Rom)', 'V27': 'Otro (contenedor, carpa, embarcación, vagón, cueva, refugio natural, etc.)', 'V28': 'Ocupada con personas presentes', 'V29': 'Ocupada con todas las personas ausentes', 'V30': 'Vivienda temporal (para vacaciones, trabajo, etc.)', 'V31': 'Desocupada', 'V32': 'Hogares', 'V33': 'A', 'V34': 'B', 'V35': 'Estrato 1', 'V36': 'Estrato 2', 'V37': 'Estrato 3', 'V38': 'Estrato 4', 'V39': 'Estrato 5', 'V40': 'Estrato 6', 'V41': 'No sabe o no tiene estrato', 'V42': 'C', 'V43': 'D', 'V44': 'E', 'V45': 'F', 'V46': 'G', 'V47': 'H', 'V48': 'J', 'V49': 'K', 'V50': 'L', 'V51': 'M', 'V52': 'N', 'V53': 'O', 'V54': 'P', 'V55': 'Q', 'V56': 'Total personas', 'V57': 'Hombres', 'V58': 'Mujeres', 'V59': '0 a 9 años', 'V60': '10 a 19 años', 'V61': '20 a 29 años', 'V62': '30 a 39 años', 'V63': '40 a 49 años', 'V64': '50 a 59 años', 'V65': '60 a 69 años', 'V66': '70 a 79 años', 'V67': '80 años o más', 'V68': 'Ninguno (Educacion)', 'V69': 'Sin Información (Educacion)', 'V70': 'Preescolar - Prejardin, Básica primaria 1 (Educacion)', 'V71': 'Básica secundaria 6, Media tecnica 10, Normalista 10 (Educacion)', 'V72': 'Técnica profesional 1 año, Tecnológica 1 año, Universitario 1 año (Educacion)', 'V73': 'Especialización 1 año, Maestria 1 año, Doctorado 1 año (Educacion)'},inplace=True)
    return df
