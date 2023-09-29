import streamlit as st
import pandas as pd
import folium
import shapely.wkt as wkt
from streamlit_folium import st_folium
from bs4 import BeautifulSoup
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, ColumnsAutoSizeMode, AgGridTheme
import streamlit.components.v1 as components

from scripts.getdata import getinfopredioscapital,streetviewapi

def style_lote(feature):
    return {
        'fillColor': '#003F2D',
        'color':'green',
        'weight': 1,
        #'dashArray': '5, 5'
    }  
    
def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')


def main():
    # obtener los argumentos de la url
    args = st.experimental_get_query_params()
    if 'code' in args: 
        code = args['code'][0]
    else: code = ''
    
    
    if code!='':
        datalotes, datacatastro,datashd,datainfopredios = getinfopredioscapital(code)
        
        col1, col2,col3 = st.columns([3,2,3])
        if datalotes.empty is False:
            
            with col1:
                polygon = wkt.loads(datalotes['wkt'].iloc[0]) 
                m       = folium.Map(location=[polygon.centroid.y, polygon.centroid.x], zoom_start=24,tiles="cartodbpositron")
                folium.GeoJson(polygon, style_function=style_lote).add_to(m)
                st_map  = st_folium(m,width=600,height=500)
                
            with col2:            
                img = streetviewapi(polygon.centroid.y,polygon.centroid.x)
                if img is not None:
                    st.image(img)
                    
            with col3:
                try:
                    datalotes['estrato'] = pd.to_numeric(datalotes['estrato'],errors='coerce')
                    idd = datalotes['estrato'].notnull()
                    if sum(idd)>0:
                        datalotes.loc[idd,'estrato'] = datalotes.loc[idd,'estrato'].astype(int)
                    idd = datalotes['estrato'].isnull()
                    if sum(idd)>0:
                        datalotes.loc[idd,'estrato'] = 'No aplica'
                except: pass
                try:  datalotes['areaconstruida'] = datalotes['areaconstruida'].apply(lambda x: round(x, 2))
                except: pass
                try:  datalotes['areaterreno'] = datalotes['areaterreno'].apply(lambda x: round(x, 2))
                except: pass                    
                
                
                formato = {
                    'direccion': 'Dirección',
                    'barrio': 'Barrio',
                    'estrato': 'Estrato',
                    'predios': '# de predios',
                    'areaconstruida':'Total área construida',
                    'areaterreno':'Total área de terreno',
                    'antiguedad_max':'Antiguedad',
                }
                cajones_por_fila = 1
                paso      = ''
                html_paso = ''
                index     = 0
                # Generar las cajas en el diseño
                for key,value in formato.items():
                    if key in datalotes and (datalotes[key].iloc[0] is not None or datalotes[key].iloc[0]!=''):
                        tamanoletra = '1.5rem'
                        if len(str(datalotes[key].iloc[0]))>20:
                            tamanoletra = '1.2rem'
                            
                        # Crear el bloque HTML correspondiente a cada iteración
                        bloque_html = f"""
                        <div class="col-xl-6 col-sm-6 mb-xl-0 mb-2">
                          <div class="card">
                            <div class="card-body p-3">
                              <div class="row">
                                <div class="numbers">
                                  <h3 class="font-weight-bolder mb-0" style="text-align: center;font-size: {tamanoletra};">{datalotes[key].iloc[0]}</h3>
                                  <p class="mb-0 text-capitalize" style="font-weight: 300;font-size: 1rem;text-align: center;">{value}</p>
                                </div>
                              </div>
                            </div>
                          </div>
                        </div>
                        """
                        
                        # Agregar el bloque al paso actual
                        paso += bloque_html
                        
                        # Si hemos completado dos iteraciones, anadir el paso a un contenedor
                        if (index + 1) % (cajones_por_fila * 2) == 0 or index == len(formato) - 1:
                            html_paso += f"""
                            <div class="container-fluid py-1">
                              <div class="row">
                                {paso}
                              </div>
                            </div>"""
                            paso = ''
                        index += 1
                        
                html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                  <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-icons.css" rel="stylesheet" />
                  <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-svg.css" rel="stylesheet" />
                  <link id="pagestyle" href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/soft-ui-dashboard.css?v=1.0.7" rel="stylesheet" />
                </head>
                <body>
                {html_paso}
                </body>
                </html>        
                """
                
                texto = BeautifulSoup(html, 'html.parser')
                st.markdown(texto, unsafe_allow_html=True)
                
                    
        #---------------------------------------------------------------------#
        # Graficas    
        #---------------------------------------------------------------------#
        conteo    = 0
        graph     = ""
        
        v1 = datacatastro.groupby('usosuelo')['id'].count().reset_index()
        v1.columns = ['usosuelo','value']
        v2 = datacatastro.groupby('actividad')['id'].count().reset_index()
        v2.columns = ['actividad','value']      

        lista = [
            {'labels':v1['usosuelo'].to_list(),'values':v1['value'].to_list(),'titulo':'# predios por uso de suelo','colors':['#012a2d']},
            {'labels':v2['actividad'].to_list(),'values':v2['value'].to_list(),'titulo':'# de predios por actividad del predio','colors':['#80bbad']},
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
        <div class="container-fluid py-0" style="margin-bottom: -200px;">
          <div class="row">     
          
            <div class="col-xl-6 col-sm-0 mb-xl-0 mb-0">
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
            
            <div class="col-xl-6 col-sm-0 mb-xl-0 mb-0">
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

        conteo    = 0
        graph     = ""
        
        v1 = datacatastro.groupby('rangoarea')['id'].count().reset_index()
        v1.columns = ['rangoarea','value'] 
        v1 = v1[v1['value']>0]
        
        lista = [
            {'labels':v1['rangoarea'].to_list(),'values':v1['value'].to_list(),'titulo':'# de predios por rango de área','colors':['#012a2d']},
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
        <div class="container-fluid py-0" style="margin-top: -100px;margin-bottom: -100px;">
          <div class="row">     
          
            <div class="col-xl-6 col-sm-0 mb-xl-0 mb-0">
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


        #---------------------------------------------------------------------#
        # Data seleccion
        #---------------------------------------------------------------------#
        html = """
        <!DOCTYPE html>
        <html>
        <head>
          <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-icons.css" rel="stylesheet" />
          <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-svg.css" rel="stylesheet" />
          <link id="pagestyle" href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/soft-ui-dashboard.css?v=1.0.7" rel="stylesheet" />
        </head>
        <body>
        <div class="container-fluid py-1" style="margin-top: -100px;margin-bottom: 0px;">
          <div class="row">
            <div class="col-xl-12 col-sm-6 mb-xl-0 mb-2">
              <div class="card">
                <div class="card-body p-3">
                  <div class="row">
                    <div class="numbers">
                      <h3 class="font-weight-bolder mb-0" style="text-align: center;font-size: 1.5rem;">Información de los predios</h3>
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
        
        v         = datainfopredios.groupby(['numeroChip']).agg({'numeroMatriculaInmobiliaria':'first','numeroCedulaCatastral':'first'}).reset_index()
        v.columns = ['prechip','matricula','cedulacatastral']
        dataselec = datacatastro.merge(v,on='prechip',how='left',validate='m:1')
        dataselec = dataselec[['predirecc','preaterre','preaconst','avaluocatastral','predial','prechip','matricula','cedulacatastral','actividad','usosuelo']]
        dataselec.columns = ['Dirección','Área de terreno','Área construida','Avalúo catastral','Predial','Chip','Matrícula','Cédula catastral','Actividad','Uso del suelo']
        gb = GridOptionsBuilder.from_dataframe(dataselec)
 
        gb.configure_default_column(cellStyle={'color': 'grey', 'font-size': '20px'}, resizable=True,filterable=True,sortable=True,suppressMenu=True, wrapHeaderText=True, autoHeaderHeight=True)
        custom_css = {".ag-header-cell-text": {"font-size": "20px", 'text-overflow': 'revert;', 'font-weight': 700, 'text-align':'center'},
              ".ag-theme-streamlit": {'transform': "scale(0.8)", "transform-origin": '0 0'}}
        gb.configure_selection(selection_mode="single", use_checkbox=True)
        gridOptions = gb.build()       

        response_close = AgGrid(
            dataselec,
            height=500,
            gridOptions=gridOptions,
            custom_css=custom_css,
            columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
            use_checkbox=True,
            theme=AgGridTheme.STREAMLIT,
            )
        
        if response_close['selected_rows']:
            datapaso         = dataselec[dataselec['Dirección']==response_close['selected_rows'][0]['Dirección']]
            chip             = datapaso['Chip'].iloc[0]
            datapasovigencia = datashd[datashd['chip']==chip][['vigencia','valorAutoavaluo','valorImpuesto','indPago','idSoporteTributario']]
            dataexport       = datashd[datashd['chip']==chip][['chip', 'vigencia', 'direccionPredio', 'nroIdentificacion', 'valorAutoavaluo', 'valorImpuesto', 'indPago', 'tipoPropietario', 'tipoDocumento', 'primerNombre', 'segundoNombre', 'primerApellido', 'segundoApellido', 'estadoRIT', 'fechaActInscripcion','matriculaMercantil', 'regimenTrib', 'fechaDocumento', 'telefono1', 'telefono2', 'telefono3', 'telefono4', 'telefono5', 'email1', 'email2', 'email3', 'direccion_contacto1', 'direccion_contacto2', 'direccion_contacto3']]

            output_list = datapaso.iloc[0].to_dict()
            tabla_detalle = ""
            for i,j in output_list.items():
                if j is not None:
                    tabla_detalle += f""" 
                    <tr>
                      <td style="border-top:none; border-left:none;border-right:none;border-bottom: 1px solid #ccc;">
                        <h6 class="text-sm" style="margin-bottom: -20px">{i}</h6>
                      </td>
                      <td style="border-top:none; border-left:none;border-right:none;border-bottom: 1px solid #ccc;">
                        <h6 class="text-sm" style="margin-bottom: -20px">{j}</h6>
                      </td>                    
                    </tr>     
                    """
            tabla_detalle = f"""
            <div class="tabla_principal">
                <table class="table align-items-center mb-0">
                  {tabla_detalle}
                </table>
            </div>
            """

            
            html_tabla = ""
            for _,i in datapasovigencia.iterrows():
                html_tabla += f""" 
                <tr>
                  <td class="align-middle text-center text-sm" style="border: none;padding: 8px;">
                    <h6 class="mb-0 text-sm">{i['vigencia']}</h6>
                  </td>
                  <td class="align-middle text-center text-sm" style="border: none;padding: 8px;">
                    <h6 class="mb-0 text-sm">${i['valorAutoavaluo']:,.0f}</h6>
                  </td>
                  <td class="align-middle text-center text-sm" style="border: none;padding: 8px;">
                    <h6 class="mb-0 text-sm">${i['valorImpuesto']:,.0f}</h6>
                  </td>
                  <td class="align-middle text-center text-sm" style="border: none;padding: 8px;">
                    <h6 class="mb-0 text-sm">{i['indPago']}</h6>
                  </td>         
                  <td class="align-middle text-center text-sm" style="border: none;padding: 8px;">
                     <a href="https://oficinavirtual.shd.gov.co/barcode/certificacion?idSoporte={i['idSoporteTributario']}" target="_blank">
                     <img src="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/publicimg/pdf.png" alt="link" width="20" height="20">
                     </a>                    
                  </td>
                """
            tabla_vigencia = f"""
            <div class="impuesto-table">
                <table class="table align-items-center mb-0">
                  <thead>
                    <tr style="margin-bottom: 0px;">
                      <th class="align-middle text-center" style="border-top:none; border-left:none;border-right:none;border-bottom: 1px solid #ccc;">Vigencia</th>
                      <th class="align-middle text-center" style="border-top:none; border-left:none;border-right:none;border-bottom: 1px solid #ccc;">Avaluo</th>
                      <th class="align-middle text-center" style="border-top:none; border-left:none;border-right:none;border-bottom: 1px solid #ccc;">Predial</th>
                      <th class="align-middle text-center" style="border-top:none; border-left:none;border-right:none;border-bottom: 1px solid #ccc;">Indicador</th>
                      <th class="align-middle text-center" style="border-top:none; border-left:none;border-right:none;border-bottom: 1px solid #ccc;">Link</th>
                    </tr>
                  </thead>
                  <tbody>
                  {html_tabla}
                  </tbody>
                </table>
            </div>
            """
            style = """
            <style>
                .tabla_principal {
                  max-width: 100%; 
                  max-height: 100%; 
                }              
                .impuesto-table {
                  overflow-x: auto;
                  overflow-y: auto; 
                  max-width: 100%; 
                  max-height: 400px; 
                }
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
              <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-icons.css" rel="stylesheet">
              <link href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/nucleo-svg.css" rel="stylesheet">
              <link id="pagestyle" href="https://personal-data-bucket-online.s3.us-east-2.amazonaws.com/css/soft-ui-dashboard.css?v=1.0.7" rel="stylesheet">
              <meta charset="UTF-8">
              <meta name="viewport" content="width=device-width, initial-scale=1.0">
              {style}
            </head>
            <body>
              <div class="container-fluid py-4" style="margin-bottom: -30px;">
                <div class="row">

                    <div class="col-xl-6 col-sm-6 mb-xl-0 mb-2">
                      <div class="card h-100">
                        <div class="card-body p-3">
                          <div class="container-fluid py-4">
                            <div class="row" style="margin-bottom: -30px;">
                              <div class="card-body p-3">
                                <div class="row">
                                  <div class="numbers">
                                    <h3 class="font-weight-bolder mb-0" style="text-align: center; font-size: 1.5rem;border-bottom: 0.5px solid #ccc; padding-bottom: 8px;">Información general</h3>
                                  </div>
                                </div>
                              </div>
                            </div>
                          </div>                       
                          <div class="container-fluid py-4">
                            <div class="row">
                              {tabla_detalle}
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                      
                  <div class="col-xl-6 col-sm-6 mb-xl-0 mb-2">
                    <div class="card h-100">
                      <div class="card-body p-3">  
                        <div class="container-fluid py-4">
                          <div class="row" style="margin-bottom: -30px;">
                            <div class="card-body p-3">
                              <div class="row">
                                <div class="numbers">
                                  <h3 class="font-weight-bolder mb-0" style="text-align: center; font-size: 1.5rem;border-bottom: 0.5px solid #ccc; padding-bottom: 8px;">Prediales</h3>
                                </div>
                              </div>
                            </div>
                          </div>
                        </div>                       
                        <div class="container-fluid py-4">
                          <div class="row">
                            {tabla_vigencia}
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
                
            
            #-----------------------------------------------------------------#
            # Exportar data
            #-----------------------------------------------------------------#
            col1, col2 = st.columns(2)
            with col1:
                csv = convert_df(dataexport)     
                st.download_button(
                   "Descargar datos de contacto",
                   csv,
                   "data_contacto.csv",
                   "text/csv",
                   key='data_contacto_info'
                )
                components.html(
                    """
                <script>
                const elements = window.parent.document.querySelectorAll('.stDownloadButton button')
                elements[0].style.width = '100%';
                elements[0].style.fontWeight = 'bold';
                elements[0].style.backgroundColor = '#17e88f';
                </script>
                """
                )
                