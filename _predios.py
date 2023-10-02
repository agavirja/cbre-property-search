import streamlit as st
import pandas as pd
import shapely.wkt as wkt
import folium
import streamlit.components.v1 as components
import matplotlib.pyplot as plt
from streamlit_folium import st_folium
from folium.plugins import Draw
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from sqlalchemy import create_engine 
from shapely.geometry import Polygon,Point,mapping,shape


from scripts.getdata import getdatacapital,getuso_destino,getdatacapital_sdh,getdatasnr,censodane,tipoinmuebl2PrecUso

def funfiltros(filtros):
    if st.session_state.datacatastro_origen.empty is False and filtros!=[]:
        idd = pd.DataFrame([True] * len(st.session_state.datacatastro_origen))[0]
        for key in filtros:
            variable = key['variable']
            value    = key['value']
            tipo     = key['type']
            if tipo=='<' and value is not None and value>0:
                idd = (idd) & (st.session_state.datacatastro_origen[variable]<value)
            elif tipo=='<=' and value is not None and value>0:
                idd = (idd) & (st.session_state.datacatastro_origen[variable]<=value)
            elif tipo=='>' and value is not None and value>0:
                idd = (idd) & (st.session_state.datacatastro_origen[variable]>value)              
            elif tipo=='>=' and value is not None and value>0:
                idd = (idd) & (st.session_state.datacatastro_origen[variable]>=value)
            elif tipo=='==' and value is not None:
                idd = (idd) & (st.session_state.datacatastro_origen[variable]==value)
            elif tipo=='multiselect':
                if value!=[]:
                    idd = (idd) & (st.session_state.datacatastro_origen[variable].isin(value))
            
        # Data de catastro con filtro
        st.session_state.datacatastro = st.session_state.datacatastro_origen[idd]
        
        # Data de lotes con filtro
        idd = st.session_state.datalotes_origen['barmanpre'].isin(st.session_state.datacatastro_origen[idd]['barmanpre'])
        st.session_state.datalotes = st.session_state.datalotes_origen[idd]
                
        st.rerun()

def style_function(feature):
    return {
        'fillColor': '#17E88F',
        'weight': 0,
        #'dashArray': '5, 5'
    }  
def style_lote(feature):
    return {
        'fillColor': '#003F2D',
        'color':'green',
        'weight': 1,
        #'dashArray': '5, 5'
    }  

def money2text(x):
    result = ''
    if pd.isnull(x) is False:
        try: result = f'${x:,.0f}'
        except: pass
    return result
    
def number2text(x):
    result = ''
    if pd.isnull(x) is False:
        try: result = f'{int(x):,}'
        except: pass
    return result

def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')

def main():
    formato = {
               'polygonfilter':None,
               'zoom_start':12,
               'latitud':4.652652, 
               'longitud':-74.077899,
               'datalotes':pd.DataFrame(),
               'datalotes_origen':pd.DataFrame(),
               'datacatastro':pd.DataFrame(),
               'datacatastro_origen':pd.DataFrame(),
               'datashd':pd.DataFrame(),
               'datashd_origen':pd.DataFrame(),   
               'datasnr_origen':pd.DataFrame(),
               'datamarket':pd.DataFrame(),                 
               'secion_filtro':False,
               }
    
    for key,value in formato.items():
        if key not in st.session_state: 
            st.session_state[key] = value
    
    tiposdeinmuebles = tipoinmuebl2PrecUso()

    col1, col2, col3 = st.columns([4,1,1])
    with col1:
        m = folium.Map(location=[st.session_state.latitud, st.session_state.longitud], zoom_start=st.session_state.zoom_start,tiles="cartodbpositron")
     
        draw = Draw(
                    draw_options={"polyline": False,"marker": False,"circlemarker":False,"rectangle":False,"circle":False},
                    edit_options={"poly": {"allowIntersection": False}}
                    )
        draw.add_to(m)
        
        if st.session_state.polygonfilter is not None:
            #st.write(str(st.session_state.polygonfilter))
            geojson_data                = mapping(st.session_state.polygonfilter)
            polygon_shape               = shape(geojson_data)
            centroid                    = polygon_shape.centroid
            st.session_state.latitud    = centroid.y
            st.session_state.longitud   = centroid.x
            folium.GeoJson(geojson_data, style_function=style_function).add_to(m)
            
            img_style = '''
                    <style>               
                        .property-image{
                          flex: 1;
                        }
                        img{
                            width:200px;
                            height:120px;
                            object-fit: cover;
                            margin-bottom: 2px; 
                        }
                    </style>
                    '''
            if st.session_state.datalotes.empty is False:
                for _,items in st.session_state.datalotes.iterrows():
                    poly      = items['wkt']
                    polyshape = wkt.loads(poly)
 
                    pop_actividad = ""
                    pop_usosuelo  = "" 
                    if items['actividad'] is not None:
                        if isinstance(items['actividad'], list):
                            pop_actividad = "<b>Actividad del predio:</b><br>"
                            for j in items['actividad']:
                                pop_actividad += f"""
                                &bull; {j}<br>
                                """
                                
                    if items['usosuelo'] is not None:
                        if isinstance(items['usosuelo'], list):
                            pop_usosuelo = "<b>Uso del suelo:</b><br>"
                            for j in items['usosuelo']:
                                pop_usosuelo += f"""
                                &bull; {j}<br>
                                """          
                    try:
                        if items['antiguedad_min']<items['antiguedad_max']:
                            antiguedad = f"<b> Antiguedad:</b> {items['antiguedad_min']}-{items['antiguedad_max']}<br>"
                        else:
                            antiguedad = f"<b> Antiguedad:</b> {items['antiguedad_min']}<br>"
                    except: antiguedad = ""
                    try:    estrato = f"<b> Estrato:</b> {int(items['estrato'])}<br>"
                    except: estrato = ""
                    popup_content =  f'''
                    <!DOCTYPE html>
                    <html>
                      <head>
                        {img_style}
                      </head>
                      <body>
                          <div>
                          <a href="https://cbre-property-search.streamlit.app/Analisis_de_predios?code={items['barmanpre']}" target="_blank">
                          <div class="property-image">
                              <img src="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/cbre/ZOOM_LOTE.png"  alt="property image" onerror="this.src='https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/sin_imagen.png';">
                          </div>
                          </a>
                          <b> Direccion:</b> {items['direccion']}<br>
                          <b> Área total construida:</b> {items['areaconstruida']}<br>
                          <b> Área total terreno:</b> {items['areaterreno']}<br>
                          <b> Número de predios:</b> {items['predios']}<br>
                          {pop_actividad}
                          {pop_usosuelo}
                          <b> Barrio:</b> {items['barrio']}<br>
                          {estrato}
                          {antiguedad}
                          </div>
                      </body>
                    </html>
                    '''
                    folium.GeoJson(polyshape, style_function=style_lote).add_child(folium.Popup(popup_content)).add_to(m)
            
        st_map = st_folium(m,width=1400,height=500)

        if st_map['last_clicked']:
            if 'lat' in st_map['last_clicked'] and 'lng' in st_map['last_clicked']:
                lat_select = st_map['last_clicked']['lat']
                lng_select = st_map['last_clicked']['lng']
                
        polygonType = ''
        if 'all_drawings' in st_map and st_map['all_drawings'] is not None:
            if st_map['all_drawings']!=[]:
                if 'geometry' in st_map['all_drawings'][0] and 'type' in st_map['all_drawings'][0]['geometry']:
                    polygonType = st_map['all_drawings'][0]['geometry']['type']
            
        if 'polygon' in polygonType.lower():
            coordenadas   = st_map['all_drawings'][0]['geometry']['coordinates']
            st.session_state.polygonfilter = Polygon(coordenadas[0])
            st.session_state.zoom_start = 16
            st.rerun()
           
    if st.session_state.secion_filtro is False:
        with col2:
            if st.button('Buscar predios'):
                if st.session_state.polygonfilter is not None and st.session_state.datalotes_origen.empty:
                    with st.spinner('Buscando predios'):
                        # Aca poner el condicional que si el poligono cae en bogota,  llamar la base de datos 
                        getdatacapital(str(st.session_state.polygonfilter))
                        # De lo contrario, poner la base de datos nacional
                        # base de datos mpio_ccdgo
                        # SELECT * FROM bigdata.data_bogota_lotes WHERE ST_INTERSECTS(ST_GeomFromText('POLYGON ((-74.056156 4.691276, -74.056885 4.686913, -74.0521 4.686101, -74.051285 4.690527, -74.056156 4.691276))', 4326),geometry)
                        # SELECT * FROM bigdata.data_bogota_lotes WHERE ST_CONTAINS(ST_GeomFromText('POLYGON ((-74.056156 4.691276, -74.056885 4.686913, -74.0521 4.686101, -74.051285 4.690527, -74.056156 4.691276))', 4326),geometry)
                else:
                    st.error('Primero se debe seleccionar un poligono en el mapa')

        with col3:
            if st.session_state.polygonfilter is not None:
                if st.button('Resetear Busqueda',key='polygonreset'):
                    for key,value in formato.items():
                        if key in st.session_state:
                            del st.session_state[key]
                            st.session_state[key] = value
                    st.rerun()
        components.html(
            """
        <script>
        const elements = window.parent.document.querySelectorAll('.stButton button')
        elements[0].style.backgroundColor = '#003f2d';
        elements[0].style.fontWeight = 'bold';
        elements[0].style.color = 'white';
        elements[0].style.width = '100%';
        elements[1].style.backgroundColor = '#cad1d3';
        elements[1].style.fontWeight = 'bold';
        elements[1].style.width = '100%';
        </script>
        """
        )

    if st.session_state.secion_filtro:
        dataprecuso,dataprecdestin = getuso_destino()
        precdestin_options         = sorted(dataprecdestin['tipo'].unique())
        precuso_options            = sorted(dataprecuso['tipo'].unique())
        if st.session_state.datacatastro_origen.empty is False:
            precdestin_options = st.session_state.datacatastro_origen[st.session_state.datacatastro_origen['precdestin'].notnull()]['actividad'].unique()
            precuso_options    = st.session_state.datacatastro_origen[st.session_state.datacatastro_origen['precuso'].notnull()]['usosuelo'].unique()
            precdestin_options = sorted(map(str, precdestin_options))
            precuso_options    = sorted(map(str, precuso_options))
            
                
        with col2:
            areaconst_min   = st.number_input('Área construida mínima',min_value=0,value=0)
            areaterreno_min = st.number_input('Área de terreno mínima',min_value=0,value=0)
            valor_min       = st.number_input('Valor mínimo del predio',min_value=0,value=0)
            precdestin      = st.multiselect('Tipo de actividad del predio', options=precdestin_options,default=None)
            tipoinmueble    = st.multiselect('Tipo de inmueble', options=list(tiposdeinmuebles),default=None)

        with col3:
            areaconst_max   = st.number_input('Área construida máxima',min_value=0,value=0)
            areaterreno_max = st.number_input('Área de terreno máxima',min_value=0,value=0)
            valor_max       = st.number_input('Valor máximo del predio',min_value=0,value=0)
            precuso         = st.multiselect('Tipo de uso del Lote', options=precuso_options,default=None)
                        
        precusofilter_tipoinmueble = []
        if tipoinmueble:
            for prediotype in tipoinmueble:
                precusofilter_tipoinmueble += tiposdeinmuebles[prediotype]
            if precusofilter_tipoinmueble!=[]:
                precusofilter_tipoinmueble = list(set(precusofilter_tipoinmueble))
            
        if precdestin:
            idd        = dataprecdestin['tipo'].isin(precdestin)
            precdestin = dataprecdestin[idd]['codigo'].to_list()

        if precuso:
            idd     = dataprecuso['tipo'].isin(precuso)
            precuso = dataprecuso[idd]['codigo'].to_list()

        #-----------------------------------------------------------------#
        # Filtros:
        #-----------------------------------------------------------------#
        filtros = [{'variable':'preaconst','value':areaconst_min,'type':'>='},
                   {'variable':'preaconst','value':areaconst_max,'type':'<='},
                   {'variable':'preaterre','value':areaterreno_min,'type':'>='},
                   {'variable':'preaterre','value':areaterreno_max,'type':'<='},
                   {'variable':'avaluocatastral','value':valor_min*0.7,'type':'>='},
                   {'variable':'avaluocatastral','value':valor_max,'type':'<='},
                   {'variable':'precdestin','value':precdestin,'type':'multiselect'},
                   {'variable':'precuso','value':precuso,'type':'multiselect'},
                   {'variable':'precuso','value':precusofilter_tipoinmueble,'type':'multiselect'},
                   ]
            
        col1, col2, col3 = st.columns([4,1,1])
        with col2:
            if st.button('Filtrar'):
                funfiltros(filtros)

        with col3:
            if st.session_state.polygonfilter is not None:
                if st.button('Resetear Busqueda ',key='polygon2reset'):
                    for key,value in formato.items():
                        if key in st.session_state:
                            del st.session_state[key]
                            st.session_state[key] = value
                    st.rerun()
        components.html(
            """
        <script>
        const elements = window.parent.document.querySelectorAll('.stButton button')
        elements[0].style.backgroundColor = '#003f2d';
        elements[0].style.fontWeight = 'bold';
        elements[0].style.color = 'white';
        elements[0].style.width = '100%';
        elements[1].style.backgroundColor = '#cad1d3';
        elements[1].style.width = '100%';
        </script>
        """
        )
                    
        #---------------------------------------------------------------------#
        # Data de analisis catastral
        #---------------------------------------------------------------------#
        if st.session_state.datacatastro.empty is False:
            html = """
            <!DOCTYPE html>
            <html>
            <head>
              <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-icons.css" rel="stylesheet" />
              <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-svg.css" rel="stylesheet" />
              <link id="pagestyle" href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/soft-ui-dashboard.css?v=1.0.7" rel="stylesheet" />
            </head>
            <body>
            <div class="container-fluid py-1" style="margin-top: -160px;margin-bottom: 0px;">
              <div class="row">
                <div class="col-xl-12 col-sm-6 mb-xl-0 mb-2">
                  <div class="card">
                    <div class="card-body p-3">
                      <div class="row">
                        <div class="numbers">
                          <h3 class="font-weight-bolder mb-0" style="text-align: center;font-size: 1.5rem;">Resumen catastral</h3>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
            </body>
            </html>        
            """
            texto = BeautifulSoup(html, 'html.parser')
            st.markdown(texto, unsafe_allow_html=True)
            
            datapaso = st.session_state.datacatastro[st.session_state.datacatastro['avaluocatastral']>0]
            if datapaso.empty is False:
                col1, col2 = st.columns([1,4])
                with col1:
                    tipofiltroorigen = st.selectbox('Tipo de filtro',options=['Actividad del predio','Uso del suelo'],label_visibility="hidden")
                    if tipofiltroorigen=='Actividad del predio':
                        tipofiltro  = 'actividad'
                    elif tipofiltroorigen=='Uso del suelo':
                        tipofiltro  = 'usosuelo'

                datapaso = datapaso.groupby([tipofiltro]).agg({'id':'count','preaconst':'sum','preaterre':'sum','avaluoxmt2':'median','predialxmt2':'median'}).reset_index()
                datapaso.columns = [tipofiltroorigen,'Total de predios','Total área construida','Total área de terreno','Avalúo catastral por mt2','Predial por mt2']
                datapaso = datapaso.sort_values(by='Total de predios',ascending=False)
                
                for i in ['Avalúo catastral por mt2','Predial por mt2']:
                    datapaso[i] = datapaso[i].apply(lambda x: money2text(x))
                for i in ['Total de predios','Total área construida','Total área de terreno']:
                    datapaso[i] = datapaso[i].apply(lambda x: int(x))
                    #datapaso[i] = datapaso[i].apply(lambda x: number2text(x))
                
                with col2:
                    st.write('')
                    st.write('')
                    st.dataframe(datapaso,width=1000)
                  
                    
    #-------------------------------------------------------------------------#
    # Data mercado
    #-------------------------------------------------------------------------#
    if st.session_state.polygonfilter is not None and st.session_state.datamarket.empty is False:
        html = """
        <!DOCTYPE html>
        <html>
        <head>
          <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-icons.css" rel="stylesheet" />
          <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-svg.css" rel="stylesheet" />
          <link id="pagestyle" href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/soft-ui-dashboard.css?v=1.0.7" rel="stylesheet" />
        </head>
        <body>
        <div class="container-fluid py-1" style="margin-bottom: 30px;">
          <div class="row">
            <div class="col-xl-12 col-sm-6 mb-xl-0 mb-2">
              <div class="card">
                <div class="card-body p-3">
                  <div class="row">
                    <div class="numbers">
                      <h3 class="font-weight-bolder mb-0" style="text-align: center;font-size: 1.5rem;">Oferta de inmuebles</h3>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
        </body>
        </html>        
        """
        texto = BeautifulSoup(html, 'html.parser')
        st.markdown(texto, unsafe_allow_html=True)
            
        col1, col2, col3 = st.columns([1,4,3])
    
        with col1:
            tiponegocio = st.selectbox("Tipo de negocio", options=["Venta","Arriendo"])
            disponibles = st.selectbox('url disponible',options=['Todos','Si','No'])

        if tiponegocio=='Venta':
            vardep = 'valorventa'
        if tiponegocio=='Arriendo':
            vardep = 'valorarriendo'
            
        #---------------------------------------------------------------------#
        # Filtro
        idd = st.session_state.datamarket['tiponegocio']==tiponegocio
        
        if disponibles=='Si':
            idd = (idd) & (st.session_state.datamarket['available']==1)
        if disponibles=='No':
            idd = (idd) & (st.session_state.datamarket['available']==0)
        if tipoinmueble!=[]:
            idd = (idd) & (st.session_state.datamarket['tipoinmueble'].isin(tipoinmueble))
        if areaconst_min>0:
            idd = (idd) & (st.session_state.datamarket['areaconstruida']>=areaconst_min)
        if areaconst_max>0:
            idd = (idd) & (st.session_state.datamarket['areaconstruida']<=areaconst_max)
        if valor_min>0 and tiponegocio=='Venta':
            idd = (idd) & (st.session_state.datamarket[vardep]>=valor_min)
        if valor_max>0 and tiponegocio=='Venta':
            idd = (idd) & (st.session_state.datamarket[vardep]<=valor_max)
        
        datamarket = st.session_state.datamarket[idd]
        datamarket.index = range(len(datamarket))
        datamarket = datamarket.iloc[0:200,:]
                       
        if datamarket.empty is False:
            css_format = """
                <style>
                  .mypropertys {
                    width: 100%;
                    height: 700px;
                    overflow-y: scroll;
                    text-align: center;
                    display: inline-block;
                    margin: 0px auto;
                  }
                  .property-image {
                    width: 100%;
                	   height: 250px;
                	   overflow: hidden; 
                    margin-bottom: 10px;
                  }
                  .price-info {
                    font-family: 'Roboto', sans-serif;
                    font-size: 20px;
                    margin-bottom: 2px;
                    text-align: center;
                  }
                  .caracteristicas-info {
                    font-family: 'Roboto', sans-serif;
                    font-size: 12px;
                    margin-bottom: 2px;
                    text-align: center;
                  }
                  img{
                    max-width: 100%;
                    width: 100%;
                    height:100%;
                    object-fit: cover;
                    margin-bottom: 10px; 
                  }
                </style>
            """
            imagenes = ''
            for i, inmueble in datamarket.iterrows():
            
                if isinstance(inmueble['imagen_principal'], str) and len(inmueble['imagen_principal'])>20: imagen_principal =  inmueble['imagen_principal']
                else: imagen_principal = "https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/sin_imagen.png"
                url_export   = f"https://cbre-property-search.streamlit.app/Ficha_del_inmueble?code={inmueble['id']}&tiponegocio={tiponegocio}" 
                if pd.isnull(inmueble['direccion']): direccionlabel = '<p class="caracteristicas-info">&nbsp</p>'
                else: direccionlabel = f'''<p class="caracteristicas-info">Dirección: {inmueble['direccion'][0:35]}</p>'''
                
                imagenes += f'''
                <div class="col-xl-6 col-lg-6 col-md-6 col-sm-6 mb-xl-2 mb-2">
                  <div class="card h-100">
                    <div class="card-body p-3">
                      <a href="{url_export}" target="_blank">
                        <div class="property-image">
                          <img src="{imagen_principal}" alt="property image" onerror="this.src='https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/sin_imagen.png';">
                        </div>
                      </a>
                      <p class="price-info"><b>${inmueble[vardep]:,.0f}</b></p>
                      {direccionlabel}
                      <p class="caracteristicas-info">{inmueble['tipoinmueble']}</p>
                      <p class="caracteristicas-info">Área construida: {inmueble['areaconstruida']}</p>
                    </div>
                  </div>
                </div>
                '''
            texto = f"""
                <!DOCTYPE html>
                <html>
                  <head>
                  <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-icons.css" rel="stylesheet"/>
                  <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-svg.css" rel="stylesheet"/>
                  <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/soft-ui-dashboard.css?v=1.0.7" id="pagestyle" rel="stylesheet"/>
                  {css_format}
                  </head>
                  <body>
                  <div class="mypropertys">
                  <div class="container-fluid py-4">
                    <div class="row">
                    {imagenes}
                    </div>
                  </div>
                  </div>
                  </body>
                </html>
                """
            with col2:
                texto = BeautifulSoup(texto, 'html.parser')
                st.markdown(texto, unsafe_allow_html=True)
                
            # Mapa
            with col3:
                m1 = folium.Map(location=[st.session_state.latitud, st.session_state.longitud], zoom_start=16,tiles="cartodbpositron")
    
                geojson_data = mapping(st.session_state.polygonfilter)
                folium.GeoJson(geojson_data, style_function=style_function).add_to(m1)
                            
                for i, inmueble in datamarket.iterrows():
                    if isinstance(inmueble['imagen_principal'], str) and len(inmueble['imagen_principal'])>20: imagen_principal =  inmueble['imagen_principal']
                    else: imagen_principal = "https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/sin_imagen.png"
                    url_export   = f"https://cbre-property-search.streamlit.app/Ficha_del_inmueble?code={inmueble['id']}&tiponegocio={tiponegocio}" 
                    string_popup = f'''
                    <!DOCTYPE html>
                    <html>
                      <head>
                        {img_style}
                      </head>
                      <body>
                          <div>
                          <a href="{url_export}" target="_blank">
                          <div class="property-image">
                              <img src="{imagen_principal}"  alt="property image" onerror="this.src='https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/sin_imagen.png';">
                          </div>
                          </a>
                          <b> Tipo de inmueble: {inmueble['tipoinmueble']}</b><br>
                          <b> Direccion: {inmueble['direccion']}</b><br>
                          <b> Precio: ${inmueble[vardep]:,.0f}</b><br>
                          <b> Área: {inmueble['areaconstruida']}</b><br>
                          </div>
                      </body>
                    </html>
                    '''
                    folium.Marker(location=[inmueble["latitud"], inmueble["longitud"]], popup=string_popup).add_to(m1)
                st_map1 = st_folium(m1,width=500,height=700)
                
            
    #-------------------------------------------------------------------------#
    # Data super notariado y registro
    #-------------------------------------------------------------------------#             
    if st.session_state.datasnr_origen.empty is False:
        html = """
        <!DOCTYPE html>
        <html>
        <head>
          <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-icons.css" rel="stylesheet" />
          <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-svg.css" rel="stylesheet" />
          <link id="pagestyle" href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/soft-ui-dashboard.css?v=1.0.7" rel="stylesheet" />
        </head>
        <body>
        <div class="container-fluid py-1" style="margin-bottom: 30px;">
          <div class="row">
            <div class="col-xl-12 col-sm-6 mb-xl-0 mb-2">
              <div class="card">
                <div class="card-body p-3">
                  <div class="row">
                    <div class="numbers">
                      <h3 class="font-weight-bolder mb-0" style="text-align: center;font-size: 1.5rem;">Inmuebles vendidos</h3>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
        </body>
        </html>        
        """
        texto = BeautifulSoup(html, 'html.parser')
        st.markdown(texto, unsafe_allow_html=True)
        #getdatasnr(str(st.session_state.polygonfilter))
        
        df = st.session_state.datasnr_origen[['docid','fecha_documento_publico','cuantia','precuso']]
        if precusofilter_tipoinmueble!=[]:
            idd = df['precuso'].isin(precusofilter_tipoinmueble)
            df  = df[idd]
        
        df         = df.sort_values(by=['docid','cuantia'],ascending=False).drop_duplicates(subset='docid',keep='first')
        df         = df[['fecha_documento_publico','cuantia']]
        df         = df.set_index(['fecha_documento_publico'])
        g          = df.groupby(pd.Grouper(freq='Y'))
        d1         = g.count()
        d1.columns = ['transacciones']
        d2         = g.sum()
        d2.columns = ['valor']
        df         = d1.merge(d2,left_index=True, right_index=True)
        df['year'] = df.index.year
        
        conteo    = 0
        graph     = ""
        colorgen  = ['#012a2d']
        coloredad = ['#80bbad']
        #colorgen  = ['rgba(0, 63, 45, 0.9)', 'rgba(0, 73, 53, 0.7)', 'rgba(0, 83, 61, 0.5)', 'rgba(0, 93, 69, 0.3)', 'rgba(0, 103, 77, 0.1)']
        #coloredad = ['rgba(0, 0, 128, 0.8)', 'rgba(0, 0, 139, 0.8)', 'rgba(0, 0, 205, 0.8)', 'rgba(65, 105, 225, 0.8)', 'rgba(70, 130, 180, 0.8)', 'rgba(135, 206, 235, 0.8)', 'rgba(173, 216, 230, 0.8)', 'rgba(240, 248, 255, 0.8)', 'rgba(255, 255, 255, 0.8)']

        lista = [
            {'labels':df['year'].to_list(),'values':df['transacciones'].to_list(),'titulo':'Número de transacciones por año','colors':colorgen},
            {'labels':df['year'].to_list(),'values':df['valor'].to_list(),'titulo':'Valor de las transacciones por año','colors':coloredad},
                 ]
        
        for item in lista:
            conteo     += 1

            graph += f'''
            const labels{conteo} = {item['labels']};
            const data{conteo} = {item['values']};
            const backgroundColors{conteo} =  {item['colors']};
            const ctx{conteo} = document.getElementById('chart{conteo}').getContext('2d');
            
            new Chart(ctx{conteo}, {{
                type: 'bar',
                data: {{
                    labels: labels{conteo},
                    datasets: [{{
                        label: '{item['titulo']}',
                        data: data{conteo},
                        backgroundColor: backgroundColors{conteo},
                        borderWidth: 0
                    }}]
                }},
                options: {{                   
                    scales: {{
                        x: {{
                            grid: {{
                                display: false
                            }}
                        }},
                        y: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{
                                    return value;
                                }}
                            }}
                        }}
                    }},
                    plugins: {{
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    return context.parsed.y;
                                }}
                            }}
                        }},
                        datalabels: {{
                            anchor: 'end', 
                            align: 'end', 
                            font: {{ size: 12, weight: 'bold' }}, 
                            color: 'black', 
                            formatter: function(value) {{
                                return value; // Esta función muestra el valor de la barra
                            }}                            
                        }}
                    }}
                }}
            }});
            '''
        graph = BeautifulSoup(graph, 'html.parser')
        style = """
        <style>
            .chart-container {
              display: flex;
              justify-content: center;
              align-items: center;
              height: 100%;
              width: 100%; 
              margin-top:100px;
            }
            body {
                font-family: Arial, sans-serif;
            }
            
            canvas {
                max-width: 100%;
                max-height: 100%;
                max-height: 300px;
            }
        </style>
        """
        html = f"""
        <!DOCTYPE html>
        <html lang="es">
        <head>
            <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-icons.css" rel="stylesheet" />
            <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-svg.css" rel="stylesheet" />
            <link id="pagestyle" href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/soft-ui-dashboard.css?v=1.0.7" rel="stylesheet" />
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            {style}
        </head>
        <body>
        <div class="container-fluid py-0" style="margin-bottom: 0px;">
          <div class="row">     
            <div class="col-md-12 col-lg-6 mb-3">
              <div class="card h-100">
                <div class="card-body p-3">  
                  <div class="numbers">
                    <div class="chart chart-container">
                      <canvas id="chart1"></canvas>
                    </div> 
                  </div>                      
                </div>
              </div>
            </div>
            
            <div class="col-md-12 col-lg-6 mb-3">
              <div class="card h-100">
                <div class="card-body p-3">  
                  <div class="numbers">  
                    <div class="chart chart-container">
                      <canvas id="chart2"></canvas>
                    </div> 
                  </div>                     
                </div>
              </div>
            </div>
          </div>
        </div>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels"></script>
        <script>
        {graph}
        </script>
        </body>
        </html>
        """
        st.components.v1.html(html, height=500)

        col1,col2 = st.columns([2,3])
        if  st.session_state.datalotes.empty is False:
            df = st.session_state.datasnr_origen[['docid','fecha_documento_publico','cuantia','precuso','barmanpre']]
            if precusofilter_tipoinmueble!=[]:
                idd = df['precuso'].isin(precusofilter_tipoinmueble)
                df  = df[idd]
            idd = st.session_state.datalotes['barmanpre'].isin(df['barmanpre'])
            
            if sum(idd)>0:
                m1 = folium.Map(location=[st.session_state.latitud, st.session_state.longitud], zoom_start=st.session_state.zoom_start,tiles="cartodbpositron")
                for _,items in st.session_state.datalotes[idd].iterrows():
                    poly      = items['wkt']
                    polyshape = wkt.loads(poly)
                    folium.GeoJson(polyshape, style_function=style_lote).add_to(m1)
                with col1:
                    st_map1 = st_folium(m1,width=600,height=500)        
        
        latitud,longitud = None,None
        if 'last_object_clicked' in st_map1 and st_map1['last_object_clicked']:
            if 'lat' in st_map1['last_object_clicked'] and 'lng' in st_map1['last_object_clicked']:
                latitud  = st_map1['last_object_clicked']['lat']
                longitud = st_map1['last_object_clicked']['lng']
        if longitud is not None and latitud is not None:
            idd = st.session_state.datalotes['wkt'].apply(lambda x: wkt.loads(x).contains( Point(longitud,latitud)))
            if sum(idd)>0:
                with col2:
                    idd = st.session_state.datasnr_origen['barmanpre']==st.session_state.datalotes[idd]['barmanpre'].iloc[0]
                    df  = st.session_state.datasnr_origen[idd]
                    df  = df.sort_values(by=['fecha_documento_publico'],ascending=False)
                    df['grupo'] = pd.factorize(df['docid'])[0] + 1
                    df          = df.sort_values(by=['grupo','fecha_documento_publico','cuantia'],ascending=[True,False,False])
                    df.index = range(len(df))
                    df = df[['grupo','fecha_documento_publico','direccion','matricula_completa','chip','preaconst','actividad','nombre','tarifa','cuantia','tipo_documento_publico','numero_documento_publico','url']]
                    st.dataframe(df)
    #-------------------------------------------------------------------------#
    # Data censo del dane
    #-------------------------------------------------------------------------#  
    if st.session_state.secion_filtro and st.session_state.polygonfilter is not None:
        datacensodane = censodane(str(st.session_state.polygonfilter))
        if datacensodane.empty is False:
            html = """
            <!DOCTYPE html>
            <html>
            <head>
              <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-icons.css" rel="stylesheet" />
              <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-svg.css" rel="stylesheet" />
              <link id="pagestyle" href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/soft-ui-dashboard.css?v=1.0.7" rel="stylesheet" />
            </head>
            <body>
            <div class="container-fluid py-1" style="margin-bottom: 30px;">
              <div class="row">
                <div class="col-xl-12 col-sm-12 mb-xl-0 mb-2">
                  <div class="card">
                    <div class="card-body p-3">
                      <div class="row">
                        <div class="numbers">
                          <h3 class="font-weight-bolder mb-0" style="text-align: center;font-size: 1.5rem;">Datos demográficos</h3>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
            </body>
            </html>        
            """
            texto = BeautifulSoup(html, 'html.parser')
            st.markdown(texto, unsafe_allow_html=True)         
            
            
            conteo    = 0
            graph     = ""
            vargen    = ['Total viviendas','Hogares','Total personas','Hombres','Mujeres']
            varedad   = ['0 a 9 años', '10 a 19 años', '20 a 29 años', '30 a 39 años', '40 a 49 años', '50 a 59 años', '60 a 69 años', '70 a 79 años', '80 años o más']
            colorgen  = ['#012a2d']
            coloredad = ['#80bbad']
            #colorgen  = ['rgba(0, 63, 45, 0.9)', 'rgba(0, 73, 53, 0.7)', 'rgba(0, 83, 61, 0.5)', 'rgba(0, 93, 69, 0.3)', 'rgba(0, 103, 77, 0.1)']
            #coloredad = ['rgba(0, 0, 128, 0.8)', 'rgba(0, 0, 139, 0.8)', 'rgba(0, 0, 205, 0.8)', 'rgba(65, 105, 225, 0.8)', 'rgba(70, 130, 180, 0.8)', 'rgba(135, 206, 235, 0.8)', 'rgba(173, 216, 230, 0.8)', 'rgba(240, 248, 255, 0.8)', 'rgba(255, 255, 255, 0.8)']


            lista = [
                {'labels':vargen,'values':datacensodane[vargen].iloc[0].to_list(),'titulo':'Cifras generales','colors':colorgen},
                {'labels':varedad,'values':datacensodane[varedad].iloc[0].to_list(),'titulo':'Por rango de edad','colors':coloredad},
                     ]
            
            for item in lista:
                conteo     += 1
    
                graph += f'''
                const labels{conteo} = {item['labels']};
                const data{conteo} = {item['values']};
                const backgroundColors{conteo} =  {item['colors']};
                const ctx{conteo} = document.getElementById('chart{conteo}').getContext('2d');
                
                new Chart(ctx{conteo}, {{
                    type: 'bar',
                    data: {{
                        labels: labels{conteo},
                        datasets: [{{
                            label: '{item['titulo']}',
                            data: data{conteo},
                            backgroundColor: backgroundColors{conteo},
                            borderWidth: 0
                        }}]
                    }},
                    options: {{                   
                        scales: {{
                            x: {{
                                grid: {{
                                    display: false
                                }}
                            }},
                            y: {{
                                beginAtZero: true,
                                ticks: {{
                                    callback: function(value) {{
                                        return value;
                                    }}
                                }}
                            }}
                        }},
                        plugins: {{
                            tooltip: {{
                                callbacks: {{
                                    label: function(context) {{
                                        return context.parsed.y;
                                    }}
                                }}
                            }},
                            datalabels: {{
                                anchor: 'end', 
                                align: 'end', 
                                font: {{ size: 12, weight: 'bold' }}, 
                                color: 'black', 
                                formatter: function(value) {{
                                    return value; // Esta función muestra el valor de la barra
                                }}                            
                            }}
                        }}
                    }}
                }});
                '''
            graph = BeautifulSoup(graph, 'html.parser')
            style = """
            <style>
                .chart-container {
                  display: flex;
                  justify-content: center;
                  align-items: center;
                  height: 100%;
                  width: 100%; 
                  margin-top:100px;
                }
                body {
                    font-family: Arial, sans-serif;
                }
                
                canvas {
                    max-width: 100%;
                    max-height: 100%;
                    max-height: 300px;
                }
            </style>
            """
            html = f"""
            <!DOCTYPE html>
            <html lang="es">
            <head>
                <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-icons.css" rel="stylesheet" />
                <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-svg.css" rel="stylesheet" />
                <link id="pagestyle" href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/soft-ui-dashboard.css?v=1.0.7" rel="stylesheet" />
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                {style}
            </head>
            <body>
            <div class="container-fluid py-0" style="margin-bottom: 0px;">
              <div class="row">     
                <div class="col-md-12 col-lg-6 mb-3">
                  <div class="card h-100">
                    <div class="card-body p-3">  
                      <div class="numbers">
                        <div class="chart chart-container">
                          <canvas id="chart1"></canvas>
                        </div> 
                      </div>                      
                    </div>
                  </div>
                </div>
                
                <div class="col-md-12 col-lg-6 mb-3">
                  <div class="card h-100">
                    <div class="card-body p-3">  
                      <div class="numbers">  
                        <div class="chart chart-container">
                          <canvas id="chart2"></canvas>
                        </div> 
                      </div>                     
                    </div>
                  </div>
                </div>
              </div>
            </div>
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels"></script>
            <script>
            {graph}
            </script>
            </body>
            </html>
            """
            st.components.v1.html(html, height=500)
            #st.write(str(BeautifulSoup(html, 'html.parser').prettify()))
            #st.dataframe(datacensodane)
            
            
    #-------------------------------------------------------------------------#
    # Descargar data
    #-------------------------------------------------------------------------#  

    col1, col2 = st.columns(2)
    if st.session_state.datashd_origen.empty is False:
        with col1:
            csv = convert_df(st.session_state.datashd_origen)     
            st.download_button(
               "Descargar datos de propietarios",
               csv,
               "data_propietarios.csv",
               "text/csv",
               key='data_propietarios'
            )
            components.html(
                """
            <script>
            const elements = window.parent.document.querySelectorAll('.stDownloadButton button')
            elements[0].style.width = '100%';
            elements[0].style.fontWeight = 'bold';
            elements[0].style.backgroundColor = '#17e88f';
            elements[1].style.width = '100%';
            elements[1].style.fontWeight = 'bold';
            elements[1].style.backgroundColor = '#17e88f';
            </script>
            """
            )
    if st.session_state.datasnr_origen.empty is False:
        with col2:
            csv = convert_df(st.session_state.datasnr_origen)     
            st.download_button(
               "Descargar datos de transacciones",
               csv,
               "data_transacciones.csv",
               "text/csv",
               key='data_transacciones'
            )
            components.html(
                """
            <script>
            const elements = window.parent.document.querySelectorAll('.stDownloadButton button')
            elements[0].style.width = '100%';
            elements[0].style.fontWeight = 'bold';
            elements[0].style.backgroundColor = '#17e88f';
            elements[1].style.width = '100%';
            elements[1].style.fontWeight = 'bold';
            elements[1].style.backgroundColor = '#17e88f';
            </script>
            """
            )
        
    #if st.session_state.datacatastro.empty is False:
    #    st.dataframe(st.session_state.datacatastro)
    #if st.session_state.datashd_origen.empty is False:
    #    idd = st.session_state.datashd_origen['chip'].isin(st.session_state.datacatastro['prechip'])
    #    st.dataframe(st.session_state.datashd_origen[idd])
    #if st.session_state.datasnr_origen.empty is False:
    #    idd = st.session_state.datasnr_origen['chip'].isin(st.session_state.datacatastro['prechip'])
    #    st.dataframe(st.session_state.datasnr_origen[idd])