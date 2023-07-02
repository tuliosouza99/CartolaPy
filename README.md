# CartolaPy

Aplicativo Streamlit para visualização e análise de dados extraídos da API oficial do Cartola FC.

## Instalação e Execução

1. Clone o repositório
2. Crie um ambiente virtual e instale as dependências
```bash
conda create -n cartolapy python=3.10
conda activate cartolapy
pip install -r requirements.txt
```

3. Execute o comando `streamlit run CartolaPy.py` e clique no botão **Atualizar Tabelas** para utilizar o aplicativo.

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
