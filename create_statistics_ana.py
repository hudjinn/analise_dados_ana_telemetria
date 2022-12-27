import datetime
import time
import pandas as pd
import numpy as np
import os

root = os.path.dirname(os.path.abspath(__file__))
dir_estacoes_ana = '/export/ANA/'
lista_estacoes = os.listdir(root+dir_estacoes_ana)
tabela_incluir = 'estacao_ana_incluir_bd_funceme.csv'


df_incluir = pd.read_csv(tabela_incluir, dtype={'CodEstacao': object})
df_incluir = df_incluir.drop(columns=['Unnamed: 0', 'Unnamed: 0.1'])
df_incluir = df_incluir.set_index('CodEstacao')
df_dados = pd.DataFrame() #df para incluir colunas para acrescentar ao df_incluir no final
count = 1
#lista_estacoes = sorted(lista_estacoes)
for estacao in lista_estacoes:
    #estacao = 'ana_51560000.csv'
    print(f'{count}/{len(lista_estacoes)} : {estacao}')
    
    #contador de iteraçções
    count +=1
    
    #ler e tratar arquivo
    df = pd.read_csv(root+dir_estacoes_ana+estacao)
    df = df.drop(columns='Unnamed: 0')


    
    #calcular intervalo entre dados
    df['DataHora'] = pd.to_datetime(df['DataHora'],yearfirst=True, format='%Y/%m/%d %H:%M:%S')
    df['Intervalo'] = df['DataHora'] - df['DataHora'].shift(-1)
    df['Intervalo'] = df['Intervalo'].dt.total_seconds().div(60)
    
    
    #construir metadados
    cod_estacao = df['CodEstacao'].iloc[0].astype(str) #converter para string para usar zfill
    cod_estacao_8_digitos = cod_estacao.zfill(8) #acrescentando zeros para obter codigo com 8 caracteres
    
    #contagem de dados numéricos
    cont = df.count(numeric_only=True) 
    total_cont = cont['CodEstacao']
    chuva_cont = cont['Chuva']
    vazao_cont = cont['Vazao']
    nivel_cont = cont['Nivel']
    
    
    
    #estatisticas precipitacao (chuva)    
    df_chuva = df.dropna(subset=['Chuva']) # apagando nan na coluna chuva

    # data da primeira e ultima informação
    
    primeiro_dado = df['DataHora'].iloc[-1]
    ultimo_dado = df['DataHora'].iloc[0] 

    #calcular moda do intervalo 
    intervalo = int(df['Intervalo'].mode().iloc[0])
    
    # para fins estatísticos
    time_range_intervalo = len(pd.date_range(start=(primeiro_dado.replace(hour=0, minute=0)), end=(ultimo_dado.replace(hour=23, minute=59)), freq=f'{intervalo}T'))
    time_range_dia = len(pd.date_range(start=primeiro_dado, end=ultimo_dado, freq='D'))

    reg_em_dias = total_cont*(intervalo*0.000694444)

    try:
        porcent_dados = ((total_cont * 100) / time_range_intervalo).round(2)
    except:
        porcent_dados = 0
    
    df_temp = pd.DataFrame([(cod_estacao_8_digitos,
                            cod_estacao,
                            intervalo,
                            
                            primeiro_dado,
                            ultimo_dado,
                            time_range_dia,
                            reg_em_dias,
                            porcent_dados,
                            
                            total_cont,
                            vazao_cont,
                            nivel_cont,
                            chuva_cont)],
                        
                            index=([cod_estacao_8_digitos]),
                        
                            columns=('CodEstacao',
                                    'Codigo da Estacao usado na consulta URL na base ANA',
                                    'Intervalo', 
                                    
                                    'Data/Hora do primeiro Registro',
                                    'Data/Hora do último Registro',
                                    'Quantidade de Dias do intervalo',
                                    'Quantidade de dias com dados (Total de Dados / Intervalo (convertido em dia)',
                                    'Confiabilidade da Estação (Porcentagem de Registros entre data do primeiro e o último Registro)',
                                    
                                    'Total de Registros', 
                                    'Total de Registros Vazão', 
                                    'Total de Registros Nível', 
                                    'Total de Registros Chuva'))

    if not df_chuva.empty:
        ultimo_dado_chuva = df_chuva['DataHora'].iloc[0]
        primeiro_dado_chuva = df_chuva['DataHora'].iloc[-1]
        mask = (df['DataHora'] >= primeiro_dado_chuva) & (df['DataHora'] <= ultimo_dado_chuva)
        
        #contar total de dados numéricos dentro do intervalo de datas com valores de precipitacao
        total_valid = df.loc[mask].count(numeric_only=True)
        

        #contar o total de dados vazios dentro do intervalo
        df_chuva_with_nan = df.loc[mask]
        
        #calcular o total de dados para o intervalo entre datas para fins estatísticos

        time_range_chuva_intervalo = len(pd.date_range(start=(primeiro_dado_chuva.replace(hour=0, minute=0)), end=(ultimo_dado_chuva.replace(hour=23, minute=59)), freq=f'{intervalo}T'))
        time_range_chuva_dia = len(pd.date_range(start=primeiro_dado_chuva, end=ultimo_dado_chuva, freq='D'))  
        reg_em_dias_chuva = chuva_cont*(intervalo*0.000694444)
        
        porcent_chuva = ((chuva_cont* 100) / time_range_chuva_intervalo).round(2)           
                                                             
        df_temp['Data/Hora do Primeiro Registro de Chuva'] = primeiro_dado_chuva 
        df_temp['Data/Hora do Último Registro de Chuva'] = ultimo_dado_chuva,
        df_temp['Quantidade de Dias do intervalo de Chuva'] = time_range_chuva_dia
        df_temp['Quantidade de dias com dados de Chuva (Total de Dados de Chuva / Intervalo (convertido em dia)'] = reg_em_dias_chuva
        df_temp['Confiablidade da Estação para dados de Chuva (Porcentagem de Registros de Chuva entre Data/Hora do primeiro e último registro de Chuva'] = porcent_chuva


    #criar indicadores
    if chuva_cont != 0:
        df_temp['Chuva'] = 1
    if vazao_cont != 0:
        df_temp['Vazão'] = 1
    if nivel_cont !=0:
        df_temp['Nivel'] = 1
    
    df_temp = df_temp.set_index('CodEstacao')
    if df_dados.empty:
        df_dados = df_temp
    else:
        df_dados = pd.concat([df_dados, df_temp])

df_final = df_incluir.join(df_dados)

df_final.to_csv('estatisticas_estacoes_ANA.csv')
