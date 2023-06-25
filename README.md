# CartolaPy

Aplicativo Streamlit para visualização e análise de dados do Cartola FC

## Instalação e Execução

1. Clone o repositório
2. Crie um ambiente virtual e instale as dependências
```bash
conda create -n cartolapy python=3.10
conda activate cartolapy
pip install -r requirements.txt
```

3. Prepare os dados
    - Caso a temporada do Brasileirão já tenha iniciado, ou você tenha esquecido de atualizar os dados antes de uma determinada rodada, baixe a versão mais recente das tabelas referentes a uma determinada temporada [neste link](). Em seguida, coloque-as na pasta `data/` na raiz do projeto.

    - Caso o Brasileirão ainda não tenha iniciado e o mercado já esteja aberto para a temporada, execute o comando `bash pre_season_script.sh`.

    Para ambos os casos, é esperado que a pasta `data/` contenha a seguinte estrutura:
    ```
    data/
    ├── csv/
    │   ├── pontos_cedidos/
    │   │   ├── 1.csv
    │   │   ├── 2.csv
    │   │   ├── 3.csv
    │   │   ├── 4.csv
    │   │   └── 5.csv
    │   ├── atletas.csv
    │   ├── clubes.csv
    │   ├── confrontos.csv
    │   ├── mandos.csv
    |   ├── pontuacoes.csv
    │   └── posicoes.csv
    ├── json/
    │   ├── clubes.json
    |   ├── posicoes.json
    │   └── status.json
    |── parquet/
    |   └── scouts.parquet
    ```

4. Execute o aplicativo
```bash
streamlit run CartolaPy.py
```

## Funcionalidades

🚀 Veja a **média**, **média básica** (média sem considerar os scouts G, A, FT, PP, DP, SG, CV e GC) e **desvio padrão** das pontuações dos atletas em diferentes condições: atuando como **mandante**, atuando como **visitante** e/ou em um **intervalo** X de rodadas.

<details>
<summary>Dica</summary>

Você pode combinar o intervalo de rodadas com as condições de mandante e visitante para obter informações mais específicas, como as pontuações de um jogador nas últimas 3 rodadas!
</details>
<br>

🧪 Filtre os jogadores por **posição**, **clube**, **status** (provável, dúvida, suspenso, contundido ou nulo) e/ou **preço** (em cartoletas).

📈 Compare os **scouts** de até **5** jogadores com base nas condições de mandante, visitante e/ou intervalo de rodadas.

🧮 Veja os **pontos cedidos** por cada time para uma determinada **posição** com base nas condições de mandante, visitante e/ou intervalo de rodadas.
