
# Crypto ETL

Este projeto implementa um processo de ETL (Extração, Transformação e Carga) para obter os dados de fechamento de mercado da exchange Binance, utilizando Apache Airflow. 

## Estrutura do Projeto

```plaintext
airflow_pipeline/
    ├── dags/
    │   └── binance_dag.py
    ├── hook/
    │   └── binance_hook.py
    └── operators/
        └── binance_operator.py
```

- **dags/**: Contém o arquivo do DAG responsável por orquestrar a rotina de extração, programada para rodar a cada 1 minuto.
- **hook/**: Contém o script que realiza a consulta à API da Binance.
- **operators/**: Contém o operator personalizado que executa a tarefa de coleta de informações da API usando o Airflow.

## Configuração e Instalação

### Pré-requisitos

- Python 3.7 ou superior
- Apache Airflow 2.0 ou superior
- Biblioteca `requests` para interação com a API da Binance

## Descrição dos Componentes

### DAG (`binance_dag.py`)

Este arquivo define o DAG do Airflow que orquestra o processo de ETL. O DAG está configurado para ser executado a cada 1 minuto, garantindo que os dados mais recentes do mercado sejam extraídos continuamente.

### Hook (`binance_hook.py`)

Este script define o hook personalizado para interagir com a API da Binance. Ele encapsula a lógica de autenticação e consulta aos endpoints necessários para obter os dados de mercado.

### Operator (`binance_operator.py`)

Este arquivo contém o operator personalizado que utiliza o hook para coletar os dados da API da Binance e processá-los conforme necessário. O operator é integrado ao DAG para executar a tarefa de extração de dados.