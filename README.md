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

3. Execute o comando `streamlit run CartolaPy.py` e utilize o aplicativo.

## Funcionalidades

🚀 Veja a **média**, **média básica** (média sem considerar os scouts G, A, FT, PP, DP, SG, CV e GC) e **desvio padrão** das pontuações dos atletas em diferentes condições: atuando como **mandante**, atuando como **visitante** e/ou em um **intervalo** X de rodadas.

<details>
<summary>Dica</summary>

Você pode combinar o intervalo de rodadas com as condições de mandante e visitante para obter informações mais específicas, como as pontuações de um jogador nas últimas 3 rodadas!
</details>
<br>

![Captura de tela 2023-06-25 184045](https://github.com/tuliosouza99/CartolaPy/assets/49206513/6461a5f9-3889-4124-9ef4-1f1613ce620e)


🧪 Filtre os jogadores por **posição**, **clube**, **status** (provável, dúvida, suspenso, contundido ou nulo) e/ou **preço** (em cartoletas).

![Captura de tela 2023-06-25 183556](https://github.com/tuliosouza99/CartolaPy/assets/49206513/61ee3877-0e14-4750-ab26-13bf6f4548f7)

📈 Compare os **scouts** de até **5** jogadores com base nas condições de mandante, visitante e/ou intervalo de rodadas.

![Captura de tela 2023-06-25 183421](https://github.com/tuliosouza99/CartolaPy/assets/49206513/8643a3f8-a695-465d-9801-0ab193e39169)

🧮 Veja os **pontos cedidos** por cada time para uma determinada **posição** com base nas condições de mandante, visitante e/ou intervalo de rodadas.

![Captura de tela 2023-06-25 184131](https://github.com/tuliosouza99/CartolaPy/assets/49206513/56847862-cb85-46e4-80ea-e22f38560c6e)

