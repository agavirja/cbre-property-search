import streamlit as st
import pandas as pd
import numpy as np
import re
import json
import requests
import shapely.wkt as wkt
import copy
from sqlalchemy import create_engine 
from shapely.geometry import Polygon,Point,mapping,shape
from unidecode import unidecode
from io import BytesIO
from PIL import Image
from multiprocessing.dummy import Pool

from scripts.formato_direccion import formato_direccion



#-----------------------------------------------------------------------------#
# DATA BOGOTA
#-----------------------------------------------------------------------------#
st.cache_data
def getdatacapital(polygon):
    user     = st.secrets["user_bigdata"]
    password = st.secrets["password_bigdata"]
    host     = st.secrets["host_bigdata"]
    schema   = st.secrets["schema_bigdata"]
    
    engine       = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    datapoints   = pd.read_sql_query(f"SELECT lotcodigo FROM  bigdata.data_bogota_lotes_point WHERE ST_CONTAINS(ST_GEOMFROMTEXT('{polygon}'), POINT(longitud, latitud))" , engine)
    dataimport   = pd.DataFrame()
    datacatastro = pd.DataFrame()
    datashd      = pd.DataFrame()
    if datapoints.empty is False:
        query      = "','".join(datapoints['lotcodigo'].unique())
        query      = f" lotcodigo IN ('{query}')"        
        dataimport = pd.read_sql_query(f"SELECT lotcodigo as barmanpre, ST_AsText(geometry) as wkt FROM  bigdata.data_bogota_lotes WHERE {query}" , engine)

        # Remover vias
        query               = "','".join(datapoints['lotcodigo'].unique())
        query               = f" barmanpre IN ('{query}')"          
        datacatastro_novias = pd.read_sql_query(f"SELECT  barmanpre  FROM  bigdata.data_bogota_catastro WHERE precdestin IN ('65','66') AND {query}" , engine)
        idd          = dataimport['barmanpre'].isin(datacatastro_novias['barmanpre'])
        if sum(idd)>0:
            dataimport = dataimport[~idd]
    
    # Data catastro
    if dataimport.empty is False:
        query        = "','".join(dataimport['barmanpre'].unique())
        query        = f" barmanpre IN ('{query}')" 
        datacatastro = pd.read_sql_query(f"SELECT id,precbarrio,prenbarrio,prechip,predirecc,preaterre,preaconst,precdestin,precuso,preuvivien,preusoph,prevetustz,barmanpre,latitud,longitud,coddir,piso,estrato  FROM  bigdata.data_bogota_catastro WHERE {query} AND (precdestin<>'65')" , engine)
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
    engine.dispose()
    
    # Data shd
    if datacatastro.empty is False:
        datashd      = getdatacapital_sdh(list(datacatastro['prechip'].unique()))
        datashdmerge = datashd.copy()
        datashdmerge = datashdmerge[datashdmerge['valorAutoavaluo']>0]
        datashdmerge = datashdmerge.sort_values(by=['chip','vigencia','valorAutoavaluo'],ascending=False)
        datashdmerge = datashdmerge.groupby('chip').agg({'valorAutoavaluo':'first','valorImpuesto':'first'}).reset_index()
        datashdmerge.columns = ['prechip','avaluocatastral','predial']
        datacatastro = datacatastro.merge(datashdmerge,on='prechip',how='left',validate='m:1')
        datacatastro['avaluoxmt2']  = datacatastro['avaluocatastral']/datacatastro['preaconst']
        datacatastro['predialxmt2'] = datacatastro['predial']/datacatastro['preaconst']

    # Data owner
    if datashd.empty is False:
        searchby = datashd[datashd['nroIdentificacion'].notnull()]
        if searchby.empty is False:
            searchby['nroIdentificacion'] = searchby['nroIdentificacion'].astype(str)
            dataowner = getdataowner(list(searchby['nroIdentificacion'].unique()))
        if dataowner.empty is False:
            datashd   = datashd.merge(dataowner,on='nroIdentificacion',how='outer')
        datashd = datashd.sort_values(by=['chip','vigencia','tipoPropietario','tipoDocumento'],ascending=False)
        
    # Data SNR
    datasnr = getdatasnr(polygon)
    
    # Match propietarios SNR y SHD
    datashd = match_snr_shd_owner(datashd.copy(),datasnr.copy())
    
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
    st.session_state.datasnr_origen            = copy.deepcopy(datasnr)
    st.session_state.datasnr_origen.index      = range(len(st.session_state.datasnr_origen))

    st.session_state.zoom_start    = 16
    st.session_state.secion_filtro = True
    
    # Data market
    engine              = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    datamarket_venta    = pd.read_sql_query(f"SELECT id,direccion,available,	tipoinmueble,	areaconstruida,	valorventa,	valorarriendo,	latitud,	longitud,	inmobiliaria,	imagen_principal FROM  cbre.data_market_venta_dpto_11 WHERE ST_CONTAINS(ST_GEOMFROMTEXT('{polygon}'), geometry)" , engine)
    datamarket_arriendo = pd.read_sql_query(f"SELECT id,direccion,available,	tipoinmueble,	areaconstruida,	valorventa,	valorarriendo,	latitud,	longitud,	inmobiliaria,	imagen_principal FROM  cbre.data_market_arriendo_dpto_11 WHERE ST_CONTAINS(ST_GEOMFROMTEXT('{polygon}'), geometry)" , engine)
    
    datamarket_venta['tiponegocio']    = 'Venta'
    datamarket_arriendo['tiponegocio'] = 'Arriendo'
    
    st.session_state.datamarket       = pd.concat([datamarket_venta,datamarket_arriendo])
    st.session_state.datamarket.index = range(len(st.session_state.datamarket))
    engine.dispose()
    
    st.rerun()
    
st.cache_data
def groupcatastro(df):
    df = df.groupby(['barmanpre']).agg({'formato_direccion':'first','barmanpre':'count','prenbarrio':'first','prevetustz':['min','max'],'estrato':'median','preaconst':'sum','preaterre':'sum','usosuelo': lambda x: list(x.unique()),'actividad':lambda x: list(x.unique())}).reset_index()
    df.columns = ['barmanpre','direccion','predios','barrio','antiguedad_min','antiguedad_max','estrato','areaconstruida','areaterreno','usosuelo','actividad']
    return df

st.cache_data
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

st.cache_data
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
    datainfopredios = getdatainfopredio(list(datacatastro[datacatastro['prechip'].notnull()]['prechip'].unique()))
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
            datashd   = datashd.merge(dataowner,on='nroIdentificacion',how='outer')
        datashd = datashd.sort_values(by=['chip','vigencia','tipoPropietario','tipoDocumento'],ascending=False)
    
    datagrupada = groupcatastro(datacatastro)
    datalotes   = datalotes.merge(datagrupada,on='barmanpre',how='left',validate='m:1')
    engine.dispose()
    return datalotes,datacatastro,datashd,datainfopredios


st.cache_data
def getdatacapital_sdh(chip):
    user     = st.secrets["user_bigdata"]
    password = st.secrets["password_bigdata"]
    host     = st.secrets["host_bigdata"]
    schema   = st.secrets["schema_bigdata"]
    engine   = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    datashd  = pd.DataFrame()
    
    if isinstance(chip, list):
        df       = pd.DataFrame({'chip':chip})
        df.index = range(len(df))
        futures  = [] 
        pool     = Pool(10)
        batches  = np.array_split(df, len(df) // 1000 + 1)      
        for batch in batches:
            futures.append(pool.apply_async(readdata_sdh,args = (engine,batch, )))
        for future in futures:
            #datashd = datashd.append(future.get())
            datashd = pd.concat([datashd,future.get()])
            
    elif isinstance(chip, str):
        query   =  'chip="{chip}"'
        datashd = pd.read_sql_query(f"SELECT chip,vigencia,valorAutoavaluo,valorImpuesto,direccionPredio,nroIdentificacion,indPago,idSoporteTributario FROM bigdata.data_bogota_catastro_vigencia WHERE {query}" , engine)
    
    engine.dispose()   
    return datashd

def readdata_sdh(engine,batch):
    query = "','".join(batch['chip'])
    query = f" chip IN ('{query}')"
    data  = pd.read_sql_query(f"SELECT chip,vigencia,valorAutoavaluo,valorImpuesto,direccionPredio,nroIdentificacion,indPago,idSoporteTributario FROM bigdata.data_bogota_catastro_vigencia WHERE {query}" , engine)
    return data

st.cache_data
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
    
st.cache_data
def getdataowner(identificacion):
    user      = st.secrets["user_bigdata"]
    password  = st.secrets["password_bigdata"]
    host      = st.secrets["host_bigdata"]
    schema    = st.secrets["schema_bigdata"]
    engine    = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    dataowner = pd.DataFrame()

    if isinstance(identificacion, list):
        df       = pd.DataFrame({'nroIdentificacion':identificacion})
        df       = df[df['nroIdentificacion'].notnull()]
        df['nroIdentificacion'] = df['nroIdentificacion'].astype(str)
        df.index = range(len(df))
        futures  = [] 
        pool     = Pool(10)
        batches  = np.array_split(df, len(df) // 1000 + 1)      
        for batch in batches:
            futures.append(pool.apply_async(readdata_owner,args = (engine,batch, )))
        for future in futures:
            #dataowner = dataowner.append(future.get())
            dataowner = pd.concat([dataowner,future.get()])

    elif isinstance(identificacion, str):
        query     = 'nroIdentificacion="{identificacion}"'
        dataowner = pd.read_sql_query(f"SELECT * FROM bigdata.data_bogota_catastro_propietario WHERE {query}" , engine)
    
    engine.dispose()  
    
    if dataowner.empty is False:
        for i in [1,2,3,4,5]:
            dataowner[f'telefono{i}'] = dataowner['telefonos'].apply(lambda x: getparam(x,'numero',i-1))
        for i in [1,2,3]:
            dataowner[f'email{i}'] = dataowner['email'].apply(lambda x: getparam(x,'direccion',i-1))
        for i in [1,2,3]:
            dataowner[f'direccion_contacto{i}'] = dataowner['dirContacto'].apply(lambda x: getparam(x,'direccion',i-1))
        dataowner.drop(columns=['telefonos','email','dirContacto','dirContactoNot','aplicaDescuento','naturaleza'],inplace=True)

    return dataowner

def readdata_owner(engine,batch):
    query = "','".join(batch['nroIdentificacion'])
    query = f" nroIdentificacion IN ('{query}')"
    data  = pd.read_sql_query(f"SELECT * FROM bigdata.data_bogota_catastro_propietario WHERE {query}" , engine)
    return data

st.cache_data
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

st.cache_data 
def match_snr_shd_owner(df1,df2):
    try:
        df2['vigencia'] = df2['fecha_documento_publico'].apply(lambda x: date2year(x))
        if 'fecha_consulta' in df2: del df2['fecha_consulta']
        variables = [x for x in list(df1) if x in list(df2)]
        varrename = {}
        if variables!=[]:
            for i in variables:
                if any([w==i for w in ['chip','vigencia']]) is False:
                    varrename.update({i:f'{i}_match'})
        if varrename!={}:
            df2.rename(columns=varrename,inplace=True)
        
            variablesmatch = ['chip','vigencia']
            for key,value in  varrename.items():
                variablesmatch.append(value)
            
            df2 = df2.sort_values(by=['chip','fecha_documento_publico','vigencia'],ascending=False,na_position='last').drop_duplicates(subset=['chip','vigencia'],keep='first')
            df1 = df1.merge(df2[variablesmatch],on=['chip','vigencia'],how='left',validate='m:1')
            
            for i in variables:
                if i in df1 and f'{i}_match' in df1:
                    idd = (df1[i].isnull()) & (df1[f'{i}_match'].notnull())
                    if sum(idd)>0:
                        df1.loc[idd,i] = df1.loc[idd,f'{i}_match']
                    del df1[f'{i}_match']
                    
            # Reemplazar los valores nulos de las vigencias mayores a la ultima vigencia con informacion no nula
            w = df1[df1['nroIdentificacion'].notnull()]
            w = w[['chip','vigencia']]
            w = w.sort_values(by=['chip','vigencia'],ascending=False)
            w = w.drop_duplicates(subset='chip',keep='first')
            w.columns = ['chip','vigenca_max']
            df1       = df1.merge(w,on='chip',how='left',validate='m:1')
            idd       = df1['vigencia']>=df1['vigenca_max']
            parte1    = df1[idd]
            parte2    = df1[~idd]
            parte1    = parte1.sort_values(by=['chip','vigencia'],ascending=False)
            for i in ['nroIdentificacion', 'tipoPropietario', 'tipoDocumento', 'primerNombre', 'segundoNombre', 'primerApellido', 'segundoApellido', 'idSujeto', 'estadoRIT', 'fechaActInscripcion', 'fechaCeseActividadesBogotaS', 'fechaInicioActividadesBogota', 'fechaInscripcion', 'fechaInscripcionD', 'fecharegimenBogota', 'fecharegimenBogotaD', 'indBuzon', 'matriculaMercantil', 'regimenTrib', 'fechaDocumento', 'fechaDocumentoS', 'telefono1', 'telefono2', 'telefono3', 'telefono4', 'telefono5', 'email1', 'email2', 'email3', 'direccion_contacto1', 'direccion_contacto2', 'direccion_contacto3']:
                parte1[i] = parte1.groupby('chip')[i].fillna(method='bfill')
            df1 = pd.concat([parte1,parte2])
            if 'vigenca_max' in df1: del df1['vigenca_max']
    except: pass
    return df1

def date2year(x):
    try: return x.year
    except: return None
#-----------------------------------------------------------------------------#
# DATA SNR
#-----------------------------------------------------------------------------#
st.cache_data
def getdatasnr(polygon):
    
    # "https://oficinavirtual.shd.gov.co/barcode/certificacion?idSoporte={i['idSoporteTributario']}"

    user     = st.secrets["user_bigdata"]
    password = st.secrets["password_bigdata"]
    host     = st.secrets["host_bigdata"]
    schema   = st.secrets["schema_bigdata"]
    
    #polygon    = "POLYGON((-74.055990 4.691069, -74.056815 4.687229, -74.052107 4.686213, -74.051477 4.690352, -74.055990 4.691069))"
    engine       = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{schema}')
    datapoints   = pd.read_sql_query(f"SELECT fecha_consulta,coddir,codigo,matricula,matricula_completa,direccion,oficinaSNR,latitud,longitud FROM  bigdata.snr_matricula_geometry WHERE ST_CONTAINS(ST_GEOMFROMTEXT('{polygon}'), POINT(longitud, latitud))" , engine)
    datacompleta = pd.DataFrame()
    if datapoints.empty is False:
        datapoints = datapoints.sort_values(by=['fecha_consulta'],ascending=False).drop_duplicates(subset='matricula_completa')

        query     = "','".join(datapoints['matricula'].astype(str).unique())
        query     = f" value IN ('{query}') AND variable='matricula'"
        datadocid = pd.read_sql_query(f"SELECT docid,value as matricula FROM  bigdata.snr_data_matricula WHERE {query}" , engine)
        
        #----------#
        # ciudad 
        query         = "','".join(datadocid['docid'].astype(str).unique())
        query         = f" docid IN ('{query}')"
        datatablacomp = pd.read_sql_query(f"SELECT docid,oficina FROM  bigdata.snr_data_completa WHERE {query}" , engine)
        lng           = wkt.loads(polygon).centroid.x
        lat           = wkt.loads(polygon).centroid.y
        colombia_mpio = pd.read_sql_query(f"SELECT mpio_ccdgo FROM  bigdata.colombia_mpio WHERE ST_CONTAINS(geometry,POINT({lng},{lat}))" , engine)
        
        # D:\Dropbox\Empresa\Consultoria Data\GEORREFERENCIACION
        #data_cod_oficinas  = pd.read_pickle(r'D:\Dropbox\Empresa\Consultoria Data\SCRAPING DATA SNR RECURRENTE\oficina2codigoDANE')
        data_cod_oficinas  = pd.read_sql_query("SELECT * FROM  bigdata.snr_oficina2mpio" , engine)
        data_cod_oficinas  = data_cod_oficinas[data_cod_oficinas['mpio_ccdgo'].isin(colombia_mpio['mpio_ccdgo'])]
        if data_cod_oficinas.empty is False:
            data_cod_oficinas['oficina'] = data_cod_oficinas['oficina'].apply(lambda x: x.lower())
            idd           = datatablacomp['oficina'].isin(data_cod_oficinas['oficina'])
            datatablacomp = datatablacomp[idd]
            idd           = datadocid['docid'].isin(datatablacomp['docid'])
            datadocid     = datadocid[idd]
        #----------#
        
        if datadocid.empty is False:
            datadocid    = datadocid.drop_duplicates()
            query        = "','".join(datadocid['docid'].astype(str).unique())
            # 125,168,169  #"COMPRAVENTA","TRANSFERENCIA DE DOMINIO A TITULO DE LEASING","TRANSFERENCIA DE DOMINIO A TITULO DE LEASING HABITACIONAL DE VIVIENDA"
            query        = f" docid IN ('{query}') AND codigo IN ('125','168','169')"
            dataprocesos = pd.read_sql_query(f"SELECT docid,codigo,nombre,tarifa,cuantia FROM  bigdata.snr_tabla_procesos WHERE {query}" , engine)
            if dataprocesos.empty is False:
                dataprocesos = dataprocesos.drop_duplicates(subset=['docid','tarifa','cuantia'])
                query        = "','".join(dataprocesos['docid'].astype(str).unique())
                query        = f" docid IN ('{query}')"
                datatable    = pd.read_sql_query(f"SELECT docid, fecha_documento_publico,tipo_documento_publico, numero_documento_publico,datos_solicitante FROM  bigdata.snr_data_completa WHERE {query}" , engine)
                if datatable.empty is False:
                    datatable    = datatable.drop_duplicates(subset='docid')
                    dataprocesos = dataprocesos.merge(datatable,on='docid',how='left',validate='m:1')
                    dataprocesos['fecha_documento_publico'] = pd.to_datetime(dataprocesos['fecha_documento_publico'],errors='coerce')
                    idd           = dataprocesos['fecha_documento_publico'].isnull()
                    
                    # Los que tienen fecha nula
                    if sum(idd)>0:
                        query         = "','".join(dataprocesos[idd]['docid'].astype(str).unique())
                        query         = f" docid IN ('{query}')"
                        datatabledate = pd.read_sql_query(f"SELECT docid,documento_json FROM  bigdata.snr_data_completa WHERE {query}" , engine)
                        datatabledate['fechanotnull'] = datatabledate['documento_json'].apply(lambda x: getEXACTfecha(x))
                        datatabledate = datatabledate.drop_duplicates(subset='docid',keep='first')
                        formato_fecha = '%d-%m-%Y'
                        datatabledate['fechanotnull'] = pd.to_datetime(datatabledate['fechanotnull'],format=formato_fecha,errors='coerce')
                        dataprocesos  = dataprocesos.merge(datatabledate[['docid','fechanotnull']],how='left',validate='m:1')
                        idd = (dataprocesos['fecha_documento_publico'].isnull()) & (dataprocesos['fechanotnull'].notnull())
                        if sum(idd)>0:
                            dataprocesos.loc[idd,'fecha_documento_publico'] = dataprocesos.loc[idd,'fechanotnull']
                        dataprocesos.drop(columns=['fechanotnull'],inplace=True)
                            
                        
                idd          = datadocid['docid'].isin(dataprocesos['docid'])
                datadocid    = datadocid[idd]
                idd          = datapoints['matricula'].isin(datadocid['matricula'])
                datapoints   = datapoints[idd]
                datapoints.index    = range(len(datapoints))
                datapoints['merge'] = range(len(datapoints))
                
                #-------------------------------------------------------------#
                # Data Bogota
                idd = datapoints['oficinaSNR'].str.contains('BOGOTA')
                if sum(idd)>0:
                    datamerge     = datapoints[idd]
                    query         = "','".join(datamerge['direccion'].astype(str).unique())
                    query         = f" predirecc IN ('{query}')"
                    datacatastro  = pd.read_sql_query(f"SELECT predirecc as direccion,prechip as chip,preaconst, preaterre,prevetustz, precuso, precdestin, barmanpre FROM  bigdata.data_bogota_catastro WHERE {query}" , engine)
                    datamerge     = datamerge.merge(datacatastro,on='direccion',how='outer') 
                    
                    idd = datamerge['chip'].isnull()
                    if sum(idd)>0:
                        query       = "','".join(datamerge[idd]['matricula'].astype(str).unique())
                        query       = f" numeroMatriculaInmobiliaria IN ('{query}')"
                        datapredios = pd.read_sql_query(f"SELECT numeroMatriculaInmobiliaria as matricula, numeroChip as chip FROM  bigdata.data_bogota_catastro_predio WHERE {query}" , engine)
                        if datapredios.empty is False:
                            query        = "','".join(datapredios['chip'].astype(str).unique())
                            query        = f" prechip IN ('{query}')"                            
                            datacatastro = pd.read_sql_query(f"SELECT prechip as chip, preaconst, preaterre,prevetustz, precuso, precdestin, barmanpre FROM  bigdata.data_bogota_catastro WHERE {query}" , engine)
                            datapredios  = datapredios.merge(datacatastro,on='chip',how='outer')
                            rr = {}
                            for i in datapredios.columns:
                                if 'matricula' not in i:
                                    rr.update({i:f'{i}_new'})
                            datapredios.rename(columns=rr,inplace=True)
                            datamerge     = datamerge.merge(datapredios,on='matricula',how='outer') 
                            for i in ['chip', 'preaconst', 'preaterre', 'prevetustz', 'precuso', 'precdestin']:
                                idd = (datamerge[i].isnull()) & (datamerge[f'{i}_new'].notnull())
                                if sum(idd)>0:
                                    datamerge.loc[idd,i] = datamerge.loc[idd,f'{i}_new']
                                del datamerge[f'{i}_new']
                                
                    datapoints = datapoints.merge(datamerge[['merge', 'chip', 'preaconst', 'preaterre', 'prevetustz', 'precuso', 'precdestin','barmanpre']],on='merge',how='left',validate='1:1')
                    del datapoints['merge']
                    
                    dataprecuso,dataprecdestin = getuso_destino()
                    dataprecuso.rename(columns={'codigo':'precuso','tipo':'usosuelo','descripcion':'desc_usosuelo'},inplace=True)
                    dataprecdestin.rename(columns={'codigo':'precdestin','tipo':'actividad','descripcion':'desc_actividad'},inplace=True)
                    datapoints = datapoints.merge(dataprecuso,on='precuso',how='left',validate='m:1')
                    datapoints = datapoints.merge(dataprecdestin,on='precdestin',how='left',validate='m:1')
                    datapoints['formato_direccion'] = datapoints['direccion'].apply(lambda x: formato_direccion(x))

                #-------------------------------------------------------------#
                # Propietarios
                if dataprocesos.empty is False:
                    dataprocesos = snr2owners(dataprocesos.copy())
                    if 'idSujeto' in dataprocesos:
                        dataprocesos = dataprocesos.sort_values(by=['docid', 'idSujeto'], na_position='last')
                    
                # {'codigo':'first','nombre':'first','tarifa':'first','cuantia':'sum','fecha_documento_publico':'first','tipo_documento_publico':'first','numero_documento_publico':'first'}
                variables         = {'codigo':'first','nombre':'first','tarifa':'first','cuantia':'sum','fecha_documento_publico':'first','tipo_documento_publico':'first','numero_documento_publico':'first'}
                include_variables = {'tipoDocumento': 'first', 'nroIdentificacion': 'first', 'tipoPropietario': 'first', 'primerNombre': 'first', 'segundoNombre': 'first', 'primerApellido': 'first', 'segundoApellido': 'first', 'idSujeto': 'first', 'estadoRIT': 'first', 'fechaActInscripcion': 'first', 'fechaCeseActividadesBogotaS': 'first', 'fechaInicioActividadesBogota': 'first', 'fechaInscripcion': 'first', 'fechaInscripcionD': 'first', 'fecharegimenBogota': 'first', 'fecharegimenBogotaD': 'first', 'indBuzon': 'first', 'matriculaMercantil': 'first', 'regimenTrib': 'first', 'fechaDocumento': 'first', 'fechaDocumentoS': 'first', 'fecha_consulta': 'first', 'telefono1': 'first', 'telefono2': 'first', 'telefono3': 'first', 'telefono4': 'first', 'telefono5': 'first', 'email1': 'first', 'email2': 'first', 'email3': 'first', 'direccion_contacto1': 'first', 'direccion_contacto2': 'first', 'direccion_contacto3': 'first'}
                for key,value in include_variables.items():
                    if key in dataprocesos:
                        variables.update({key:value})
                
                #-------------------------------------------------------------#
                # Data completa                           
                datacompleta = datadocid.merge(datapoints,on='matricula',how='outer')
                vardrop      = [x for x in ['fecha_consulta','codigo'] if x in datacompleta]
                if vardrop!=[]: datacompleta.drop(columns=vardrop,inplace=True)
                dataprocesos = dataprocesos.groupby('docid').agg(variables).reset_index()
                datacompleta = datacompleta.merge(dataprocesos,on='docid',how='left',validate='m:1')
                datacompleta['url'] =  datacompleta['docid'].apply(lambda x: f'https://radicacion.supernotariado.gov.co/app/static/ServletFilesViewer?docId={x}')
                datacompleta.index  = range(len(datacompleta))
                
    engine.dispose()
    return datacompleta

def getEXACTfecha(x):
    result = None
    try:
        x = json.loads(x)
        continuar = 0
        for i in ['fecha','fecha:','fecha expedicion','fecha expedicion:','fecha recaudo','fecha recaudo:']:
            for j in x:
                if i==re.sub('\s+',' ',unidecode(j['value'].lower())):
                    posicion = x.index(j)
                    result   = x[posicion+1]['value']
                    continuar = 1
                    break
            if continuar==1:
                break
    except: result = None
    if result is None:
        result = getINfecha(x)
    return result
    
def getINfecha(x):
    result = None
    try:
        x = json.loads(x)
        continuar = 0
        for i in ['fecha','fecha:','fecha expedicion','fecha expedicion:','fecha recaudo','fecha recaudo:']:
            for j in x:
                if i in re.sub('\s+',' ',unidecode(j['value'].lower())):
                    posicion = x.index(j)
                    result   = x[posicion+1]['value']
                    continuar = 1
                    break
            if continuar==1:
                break
    except: result = None
    return result
     
st.cache_data
def snr2owners(df):
    df['datos_solicitante'] = df['datos_solicitante'].apply(lambda x: data2jsonstruct(x))
    df['titular']           = df['datos_solicitante'].apply(lambda x: getvalue(x,0))
    df['email']             = df['datos_solicitante'].apply(lambda x: getvalue(x,1))
    df['tipoDocumento']     = df['datos_solicitante'].apply(lambda x: getname(x,2))
    df['nroIdentificacion'] = df['datos_solicitante'].apply(lambda x: getvalue(x,2))
    df['tipoDocumento']     = df['tipoDocumento'].replace(['cedula de ciudadania','nit','cedula de extranjeria','tarjeta de identidad','pasaporte'], ['C.C.','N.I.T.','C.E.','T.I.','PASAPORTE'])
    
    if 'datos_solicitante' in df: del df['datos_solicitante']

    identificacion = df[df['nroIdentificacion'].notnull()]
    dataowner      = pd.DataFrame()
    if identificacion.empty is False:
        identificacion['nroIdentificacion'] = identificacion['nroIdentificacion'].astype(str)
        dataowner = getdataowner(list(identificacion['nroIdentificacion'].unique()))
    
    if dataowner.empty is False:
        dataowner = dataowner.drop_duplicates(subset=['tipoDocumento','nroIdentificacion'],keep='first')
        df = df.merge(dataowner,on=['tipoDocumento','nroIdentificacion'],how='left',validate='m:1')
    return df

def data2jsonstruct(x):
    try: return json.loads(x)
    except: return None

def getvalue(x,pos):
    try: return x[pos]['value']
    except: return None
def getname(x,pos):
    try: return x[pos]['variable']
    except: return None  
#-----------------------------------------------------------------------------#
# DATA DANE
#-----------------------------------------------------------------------------#
st.cache_data
def censodane(polygon):
    try:
        # https://geoportal.dane.gov.co/geovisores/territorio/analisis-cnpv-2018/
        coordenadas = re.findall(r"(-?\d+\.\d+) (-?\d+\.\d+)", polygon)
        coordenadas = coordenadas[:-1]
        coordenadas = ",".join([f"{lon},{lat}" for lon, lat in coordenadas])
        url = f"https://geoportal.dane.gov.co/laboratorio/serviciosjson/poblacion/20221215-indicadordatospoligonos.php?coordendas={coordenadas}"
        r   = requests.get(url,verify=False, timeout=20).json()
        df  = pd.DataFrame(r)
        df.rename(columns={'V1': 'Total viviendas', 'V2': 'Uso mixto', 'V3': 'Unidad no residencial', 'V4': 'Lugar especial de alojamiento - LEA', 'V5': 'Industria (uso mixto)', 'V6': 'Comercio (uso mixto)', 'V7': 'Servicios (uso mixto)', 'V8': 'Agropecuario, agroindustrial, foresta (uso mixto)', 'V9': 'Sin información (uso mixto)', 'V10': 'Industria (uso no residencial)', 'V11': 'Comercio (uso no residencial)', 'V12': 'Servicios (uso no residencial)', 'V13': 'Agropecuario, Agroindustrial, Foresta (uso no residencial)', 'V14': 'Institucional (uso no residencial)', 'V15': 'Lote (Unidad sin construcción)', 'V16': 'Parque/ Zona Verde (uso no residencial)', 'V17': 'Minero-Energético (uso no residencial)', 'V18': 'Protección/ Conservación ambiental (uso no residencial)', 'V19': 'En Construcción (uso no residencial)', 'V20': 'Sin información (uso no residencial)', 'V21': 'Viviendas', 'V22': 'Casa', 'V23': 'Apartamento', 'V24': 'Tipo cuarto', 'V25': 'Vivienda tradicional indígena', 'V26': 'Vivienda tradicional étnica (Afrocolombiana, Isleña, Rom)', 'V27': 'Otro (contenedor, carpa, embarcación, vagón, cueva, refugio natural, etc.)', 'V28': 'Ocupada con personas presentes', 'V29': 'Ocupada con todas las personas ausentes', 'V30': 'Vivienda temporal (para vacaciones, trabajo, etc.)', 'V31': 'Desocupada', 'V32': 'Hogares', 'V33': 'A', 'V34': 'B', 'V35': 'Estrato 1', 'V36': 'Estrato 2', 'V37': 'Estrato 3', 'V38': 'Estrato 4', 'V39': 'Estrato 5', 'V40': 'Estrato 6', 'V41': 'No sabe o no tiene estrato', 'V42': 'C', 'V43': 'D', 'V44': 'E', 'V45': 'F', 'V46': 'G', 'V47': 'H', 'V48': 'J', 'V49': 'K', 'V50': 'L', 'V51': 'M', 'V52': 'N', 'V53': 'O', 'V54': 'P', 'V55': 'Q', 'V56': 'Total personas', 'V57': 'Hombres', 'V58': 'Mujeres', 'V59': '0 a 9 años', 'V60': '10 a 19 años', 'V61': '20 a 29 años', 'V62': '30 a 39 años', 'V63': '40 a 49 años', 'V64': '50 a 59 años', 'V65': '60 a 69 años', 'V66': '70 a 79 años', 'V67': '80 años o más', 'V68': 'Ninguno (Educacion)', 'V69': 'Sin Información (Educacion)', 'V70': 'Preescolar - Prejardin, Básica primaria 1 (Educacion)', 'V71': 'Básica secundaria 6, Media tecnica 10, Normalista 10 (Educacion)', 'V72': 'Técnica profesional 1 año, Tecnológica 1 año, Universitario 1 año (Educacion)', 'V73': 'Especialización 1 año, Maestria 1 año, Doctorado 1 año (Educacion)'},inplace=True)
    except: df = pd.DataFrame()
    return df

#-----------------------------------------------------------------------------#
# DATA STREETVIEW
#-----------------------------------------------------------------------------#
st.cache_data
def streetviewapi(latitud,longitud):
    API_KEY = 'AIzaSyBEjvAMTg70W6oUvWc5HzYUS3O9rzEI9Jw' #st.secrets["API_KEY"]
    tamaño = "640x480"  # Puedes ajustar el tamaño según tus necesidades
    
    # Construye la URL de la API de Google Street View
    base_url = "https://maps.googleapis.com/maps/api/streetview"
    params = {
        "size": tamaño,
        "location": f"{latitud},{longitud}",
        "key": API_KEY
    }
    
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        return Image.open(BytesIO(response.content))
        #return response.url
    else:
        return None