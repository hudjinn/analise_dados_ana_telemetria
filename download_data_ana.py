import xmltodict
import requests
import pandas as pd
import numpy as np
import os

df_stn = pd.read_csv('estacao_ana_incluir_bd.csv', dtype={'CodEstacao':object})
df_stn = df_stn.drop_duplicates()
codigos_origem = df_stn['CodEstacao'].to_list()

count = 0


for codigo_origem in codigos_origem:
    control = True
    count += 1
    while control:
        try:
            print(codigo_origem)
            path = f'export/ANA/ana_{codigo_origem}.csv'
            if os.path.isfile(path):
                print(f'{path} existe')
                control = False

            else:

                url= f'https://telemetriaws1.ana.gov.br//ServiceANA.asmx/DadosHidrometeorologicos?codEstacao={codigo_origem}&dataInicio=01/01/2013&dataFim=31/12/2022'
                print(f'{count}/{len(codigos_origem)}: {codigo_origem} <{url}>')
                a=requests.get(url)
                xml=a.text
                xml_dict = xmltodict.parse(xml)
                raw_data=xml_dict['DataTable']['diffgr:diffgram']['DocumentElement']['DadosHidrometereologicos']   
                df_temp = pd.DataFrame(data=raw_data)
                a.close
                df_temp = df_temp.drop(columns=['@diffgr:id','@msdata:rowOrder'])
                df_temp = df_temp.fillna(value=np.nan)
                print(df_temp)
                df_temp.to_csv(path)
                
            
        except:
            if type(codigo_origem) != int:
                codigo_origem = int(codigo_origem)
            else: 
                control = False
