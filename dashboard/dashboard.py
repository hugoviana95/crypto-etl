import streamlit as st
import pandas as pd
import glob
import os
import plotly.express as px
from sklearn.metrics import root_mean_squared_error

BASE_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# st.set_page_config(layout='wide')

def format(valor, prefixo=''):
    for unidade in ['', 'mil']:
        if valor < 1000:
            return f'{prefixo} {valor:.2f} {unidade}'
        valor /= 1000 
    return f'{prefixo} {valor:.2f} milhões'


st.title('Crypto Dash')

#Leitura dos dados
file_path = os.path.join(os.path.join(BASE_FOLDER, 'datalake/gold/dashboard/'), '*.csv')
csv_files = glob.glob(file_path)
files_list = []
for file in csv_files:
    files_list.append(pd.read_csv(file, parse_dates=['Close_datetime']))
if len(files_list) < 2: # Paralisa o carregamento do dashboard caso não haja dados suficientes
    st.text("Ainda não há dados suficientes.")
    st.stop()
data = pd.concat(files_list, ignore_index=True)


if data.shape[0] > 10: # Paralisa o carregamento do dashboard caso não haja dados suficientes
    
    # Filtrar moedas
    col1, col2 = st.columns([1,4])
    coin = 'btcbrl'
    with col1:
        coin = st.selectbox("Selecione a moeda",data['symbol'].unique())
        data = data.loc[data['symbol'] == coin]
    with col2:
        st.image(f'assets/{coin}.png', width=100)

    # Gráfico para acompanhar predições
    chart = px.line(data,
                    x='Close_datetime', 
                    y=['Close_price', 'Predict'],
                    # labels={'Close_price': 'Preço Real', 'Predict': 'Preço Previsto'},
                    color_discrete_map={'Close_price': 'gold', 'Predict': 'grey'})
    chart.update_layout(yaxis_title='Preço', 
                        xaxis_title='Horário',
                        legend={'title':'Legenda'})
    # Renomeando os traços na legenda
    chart.for_each_trace(lambda t: t.update(name={'Close_price': 'Real', 'Predict': 'Previsto'}.get(t.name)))
    st.plotly_chart(chart)

    # Métricas
    col1, col2 = st.columns(2)
    with col1:
        # Calculo do erro médio
        erro = root_mean_squared_error(data.dropna()['Predict'], data.dropna()['Close_price'])
        st.metric('RMSE', format(erro, 'R$'))

    with col2:
        # Previsão
        val_previsto = data.iloc[0]['Predict']
        ult_valor = data.iloc[1]['Close_price']
        delta = ((val_previsto - ult_valor)/ult_valor)*100
        st.metric(f'Previsão para {data['Close_datetime'].iloc[0]}', format(val_previsto, 'R$'), delta=f"{format(delta)}%") 
else:
    st.text("Ainda não há dados suficientes.")