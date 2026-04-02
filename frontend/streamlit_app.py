import asyncio
import warnings

import httpx
import pandas as pd
import streamlit as st

import plotter as P

warnings.filterwarnings("ignore")

BACKEND_URL = "http://localhost:8000"
PRECO_MIN = 0
PRECO_MAX = 30
RODADA_INICIAL = 1


def get(endpoint: str):
    response = httpx.get(f"{BACKEND_URL}{endpoint}", timeout=30.0)
    response.raise_for_status()
    return response.json()


def post(endpoint: str):
    response = httpx.post(f"{BACKEND_URL}{endpoint}", timeout=30.0)
    response.raise_for_status()
    return response.json()


async def main():
    st.set_page_config(layout="wide", page_icon=":tophat:")

    st.sidebar.markdown(
        "<h1 style='text-align: center;'>Cartola"
        "<span style='color: #ff6600'>Py</span></hi>",
        unsafe_allow_html=True,
    )

    try:
        market_status = get("/market/status")
    except Exception:
        st.error("Unable to connect to backend. Make sure the backend is running.")
        st.stop()

    if market_status["status_mercado"] != 1 or market_status["game_over"]:
        st.sidebar.info(
            "O mercado está fechado! Espere sua reabertura para atualizar as tabelas."
        )
        st.stop()

    rodada_atual = market_status["rodada_atual"]

    atletas_df = pd.DataFrame(get("/atletas"))
    status_dict = get("/dict/status")
    clubes_dict = get("/dict/clubes")
    posicoes_dict = get("/dict/posicoes")

    clubes_list = sorted(clubes_dict.values())
    posicoes_list = list(posicoes_dict.values())
    status_list = list(status_dict.values())

    if rodada_atual == 1:
        st.info("A temporada ainda não começou!")
        st.stop()

    media_opcao = st.sidebar.radio(
        "Selecione um Tipo de Média",
        ["Geral", "Mandante", "Visitante"],
        key="Media_ppcao",
    )

    st.title("Jogadores")
    rodadas_atletas = (1, 1)

    if rodada_atual > 1:
        rodadas_atletas = st.slider(
            "Selecione um intervalo de rodadas",
            RODADA_INICIAL,
            rodada_atual,
            (RODADA_INICIAL, rodada_atual),
            key="rodadas_atletas",
        )

    container_atletas = st.container()

    with st.expander("Filtros"):
        clubes_escolhidos = st.multiselect("Filtro por Clube", clubes_list)
        precos_escolhidos = st.slider(
            "Filtro por Preço", PRECO_MIN, PRECO_MAX, (PRECO_MIN, PRECO_MAX)
        )

        min_jogos = 1
        if rodadas_atletas[1] > rodadas_atletas[0]:
            min_jogos = st.slider(
                "Filtro por Número Mínimo de Jogos",
                RODADA_INICIAL,
                rodadas_atletas[1] - rodadas_atletas[0] + 1,
                RODADA_INICIAL,
            )

        col_posicoes, col_status = st.columns(2)
        with col_posicoes:
            posicoes_escolhidas = st.multiselect("Filtro por Posição", posicoes_list)
        with col_status:
            status_escolhidos = st.multiselect("Filtro por Status", status_list)

    with container_atletas:
        if media_opcao == "Geral":
            atletas_out_index2names, atletas_out_df = P.plot_atletas_geral(
                atletas_df.set_index("atleta_id"),
                clubes_escolhidos,
                posicoes_escolhidas,
                status_escolhidos,
                min_jogos,
                precos_escolhidos,
                rodadas_atletas,
            )
        elif media_opcao == "Mandante":
            atletas_out_index2names, atletas_out_df = P.plot_atletas_mando(
                atletas_df.set_index("atleta_id"),
                clubes_escolhidos,
                posicoes_escolhidas,
                status_escolhidos,
                min_jogos,
                precos_escolhidos,
                rodadas_atletas,
                mando_flag=1,
            )
        else:
            atletas_out_index2names, atletas_out_df = P.plot_atletas_mando(
                atletas_df.set_index("atleta_id"),
                clubes_escolhidos,
                posicoes_escolhidas,
                status_escolhidos,
                min_jogos,
                precos_escolhidos,
                rodadas_atletas,
                mando_flag=0,
            )

        st.dataframe(atletas_out_df)

    st.subheader("Comparação de Scouts")
    atletas_ids = st.multiselect(
        "Selecione até 5 IDs de Jogadores",
        options=atletas_out_index2names.keys(),
        max_selections=5,
        key="id_jogador",
    )

    if len(atletas_ids) > 0:
        if media_opcao == "Geral":
            st.plotly_chart(
                P.plot_player_scouts(
                    atletas_ids, atletas_out_index2names, rodadas_atletas
                ),
                use_container_width=True,
            )
        elif media_opcao == "Mandante":
            st.plotly_chart(
                P.plot_player_scouts(
                    atletas_ids,
                    atletas_out_index2names,
                    rodadas_atletas,
                    atletas_df=atletas_df.set_index("atleta_id"),
                    mando_flag=1,
                ),
                use_container_width=True,
            )
        else:
            st.plotly_chart(
                P.plot_player_scouts(
                    atletas_ids,
                    atletas_out_index2names,
                    rodadas_atletas,
                    atletas_df=atletas_df.set_index("atleta_id"),
                    mando_flag=0,
                ),
                use_container_width=True,
            )

    st.title("Pontos Cedidos")
    rodadas_pontos_cedidos = (1, 1)
    container_pontos_cedidos = st.container()

    if rodada_atual > 1:
        rodadas_pontos_cedidos = st.slider(
            "Selecione um intervalo de rodadas",
            RODADA_INICIAL,
            rodada_atual,
            (RODADA_INICIAL, rodada_atual),
            key="rodadas_pontos_cedidos",
        )

    posicao_escolhida = st.selectbox("Selecione uma Posição", posicoes_list)
    abreviacao2posicao = {
        "GOL": 1,
        "LAT": 2,
        "ZAG": 3,
        "MEI": 4,
        "ATA": 5,
        "TEC": 6,
    }
    pontos_cedidos_posicao = pd.DataFrame(
        get(f"/pontos-cedidos/{abreviacao2posicao[posicao_escolhida]}")
    ).set_index("clube_id")

    if media_opcao == "Geral":
        with container_pontos_cedidos:
            st.write(
                "Quantos pontos um time cede em média para um atleta de uma determinada posição."
            )

        st.dataframe(
            P.plot_pontos_cedidos_geral(pontos_cedidos_posicao, rodadas_pontos_cedidos)
        )
    elif media_opcao == "Mandante":
        with container_pontos_cedidos:
            st.write(
                "Quantos pontos um time cede em média para um atleta que joga como mandante em uma determinada posição."
            )

        st.dataframe(
            P.plot_pontos_cedidos_mando(
                pontos_cedidos_posicao, rodadas_pontos_cedidos, mando_flag=0
            )
        )
    else:
        with container_pontos_cedidos:
            st.write(
                "Quantos pontos um time cede em média para um atleta que joga como visitante em uma determinada posição."
            )
        st.dataframe(
            P.plot_pontos_cedidos_mando(
                pontos_cedidos_posicao, rodadas_pontos_cedidos, mando_flag=1
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
