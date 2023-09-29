# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 22:08:50 2023

@author: LENOVO T14
"""
df1 = datashd.copy()
df2 = datasnr.copy()

def date2year(x):
    try: return x.year
    except: return None
    
def match_snr_shd(df1,df2):
    
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
        
        parte1 = parte1.sort_values(by=['chip','vigencia'],ascending=False)
        for i in ['nroIdentificacion', 'tipoPropietario', 'tipoDocumento', 'primerNombre', 'segundoNombre', 'primerApellido', 'segundoApellido', 'idSujeto', 'estadoRIT', 'fechaActInscripcion', 'fechaCeseActividadesBogotaS', 'fechaInicioActividadesBogota', 'fechaInscripcion', 'fechaInscripcionD', 'fecharegimenBogota', 'fecharegimenBogotaD', 'indBuzon', 'matriculaMercantil', 'regimenTrib', 'fechaDocumento', 'fechaDocumentoS', 'telefono1', 'telefono2', 'telefono3', 'telefono4', 'telefono5', 'email1', 'email2', 'email3', 'direccion_contacto1', 'direccion_contacto2', 'direccion_contacto3']:
            parte1[i] = parte1.groupby('chip')[i].fillna(method='bfill')

        df1 = pd.concat([parte1,parte2])
        return df1
        