import asyncio
import os
import warnings

import pandas as pd
import numpy as np
import streamlit as st

import plotter as P
import src.utils as U
import src.pre_season.dfs_creator as dfs_creator
from src.enums import DataPath, UpdateTablesMsg
from src.pre_season.dicts_creator import create_dicts
from src.atletas_updater import update_atletas
from src.confrontos_or_mandos_updater import ConfrontosOrMandosUpdater
from src.pontos_cedidos_updater import PontosCedidosUpdater
from src.pontuacoes_updater import update_pontuacoes_and_scouts

warnings.filterwarnings('ignore')
PRECO_MIN = 0
PRECO_MAX = 30
RODADA_INICIAL = 1


async def update_tables(rodada: list[int] | int):
    await dfs_creator.create_clubes_and_posicoes()
    await asyncio.gather(create_dicts(), update_atletas())

    if isinstance(rodada, int):  # update all rounds until rodada
        await asyncio.gather(
            dfs_creator.create_pontos_cedidos_dfs(),
            asyncio.to_thread(dfs_creator.create_confrontos_or_mandos, 'confrontos'),
            asyncio.to_thread(dfs_creator.create_confrontos_or_mandos, 'mandos'),
        )
        mandos_updater = ConfrontosOrMandosUpdater('mandos')
        confrontos_updater = ConfrontosOrMandosUpdater('confrontos')

        if rodada == 1:
            await asyncio.gather(
                mandos_updater.update_table(rodada),
                confrontos_updater.update_table(rodada),
                update_pontuacoes_and_scouts(first_round=True),
            )
        else:
            await asyncio.gather(
                mandos_updater.update_table(range(1, rodada + 1)),
                confrontos_updater.update_table(range(1, rodada + 1)),
                update_pontuacoes_and_scouts(),
            )
            pontos_cedidos_updater = PontosCedidosUpdater()
            await pontos_cedidos_updater.update_pontos_cedidos(range(1, rodada))

    else:  # update all rounds in rodada
        mandos_updater = ConfrontosOrMandosUpdater('mandos')
        confrontos_updater = ConfrontosOrMandosUpdater('confrontos')
        pontos_cedidos_updater = PontosCedidosUpdater()

        await asyncio.gather(
            mandos_updater.update_table(rodada),
            confrontos_updater.update_table(rodada),
        )
        await asyncio.gather(
            update_pontuacoes_and_scouts(),
            pontos_cedidos_updater.update_pontos_cedidos([rodada_ - 1 for rodada_ in rodada]),
        )


async def main():
    st.set_page_config(layout='wide', page_icon=':tophat:')

    st.sidebar.markdown(
        "<h1 style='text-align: center;'>Cartola"
        "<span style='color: #ff6600'>Py</span></hi>",
        unsafe_allow_html=True,
    )

    mercado_json = await U.get_page_json(
        'https://api.cartola.globo.com/mercado/status'
    )

    if mercado_json['status_mercado'] != 1 or mercado_json['game_over']:
        st.sidebar.info(UpdateTablesMsg.MERCADO_FECHADO.value)

        if not all([os.path.exists(table) for table in DataPath.as_list()]):
            st.warning(UpdateTablesMsg.NOT_ALL_TABLES_FOUND.value)
            st.stop()
    else:
        if mercado_json['rodada_atual'] == 1:
            st.info(UpdateTablesMsg.SEASON_NOT_STARTED.value)
            atualizar_tabelas = st.button('Atualizar Tabelas', key='atualizar_tabelas')

            if atualizar_tabelas:
                with st.empty():
                    await update_tables(mercado_json['rodada_atual'])
                    st.success(UpdateTablesMsg.SUCCESS.value)
            st.stop()

        else:
            if not all([os.path.exists(table) for table in DataPath.as_list()]):
                with st.empty():
                    await update_tables(mercado_json['rodada_atual'])
                    st.success(UpdateTablesMsg.SUCCESS.value)
            else:
                confrontos_df = (
                    pd.read_csv('data/csv/confrontos.csv', index_col=0)
                    .set_index('clube_id')
                    .loc[
                        :, [str(i) for i in range(1, mercado_json['rodada_atual'] + 1)]
                    ]
                )
                rounds_to_update = np.where(
                    [
                        confrontos_df[str(col)].isna().all()
                        for col in confrontos_df.columns
                    ]
                )[0]
                rounds_to_update = [round_ + 1 for round_ in rounds_to_update]

                if len(rounds_to_update) > 0:
                    with st.empty():
                        await update_tables(rounds_to_update)
                        st.success(UpdateTablesMsg.SUCCESS.value)
                else:
                    atualizar_tabelas = st.sidebar.button(
                        'Atualize as informações mais recentes do mercado',
                        key='atualizar_tabelas',
                    )
                    if atualizar_tabelas:
                        with st.sidebar.empty():
                            await update_atletas()
                            await update_pontuacoes_and_scouts()
                            st.success(UpdateTablesMsg.SUCCESS.value)

    clubes_dict, posicoes_dict, status_dict = await asyncio.gather(
        U.load_dict_async('clubes'),
        U.load_dict_async('posicoes'),
        U.load_dict_async('status'),
    )
    clubes_list = sorted(clubes_dict.values())
    posicoes_list = posicoes_dict.values()
    status_list = status_dict.values()

    atletas_df = pd.read_csv('data/csv/atletas.csv', index_col=0)
    rodada_atual = int(atletas_df.at[0, 'rodada_id'])

    media_opcao = st.sidebar.radio(
        'Selecione um Tipo de Média',
        ['Geral', 'Mandante', 'Visitante'],
        key='Media_ppcao',
    )

    # Atletas
    st.title('Jogadores')
    rodadas_atletas = (1, 1)

    if rodada_atual > 1:
        rodadas_atletas = st.slider(
            'Selecione um intervalo de rodadas',
            RODADA_INICIAL,
            rodada_atual,
            (RODADA_INICIAL, rodada_atual),
            key='rodadas_atletas',
        )

    container_atletas = st.container()

    with st.expander('Filtros'):
        clubes_escolhidos = st.multiselect('Filtro por Clube', clubes_list)
        precos_escolhidos = st.slider(
            'Filtro por Preço', PRECO_MIN, PRECO_MAX, (PRECO_MIN, PRECO_MAX)
        )

        min_jogos = 1
        if rodadas_atletas[1] > rodadas_atletas[0]:
            min_jogos = st.slider(
                'Filtro por Número Mínimo de Jogos',
                RODADA_INICIAL,
                rodadas_atletas[1] - rodadas_atletas[0] + 1,
                RODADA_INICIAL,
            )

        col_posicoes, col_status = st.columns(2)
        with col_posicoes:
            posicoes_escolhidas = st.multiselect('Filtro por Posição', posicoes_list)
        with col_status:
            status_escolhidos = st.multiselect('Filtro por Status', status_list)

    with container_atletas:
        if media_opcao == 'Geral':
            atletas_out_index2names, atletas_out_df = P.plot_atletas_geral(
                atletas_df.set_index('atleta_id'),
                clubes_escolhidos,
                posicoes_escolhidas,
                status_escolhidos,
                min_jogos,
                precos_escolhidos,
                rodadas_atletas,
            )
        elif media_opcao == 'Mandante':
            atletas_out_index2names, atletas_out_df = P.plot_atletas_mando(
                atletas_df.set_index('atleta_id'),
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
                atletas_df.set_index('atleta_id'),
                clubes_escolhidos,
                posicoes_escolhidas,
                status_escolhidos,
                min_jogos,
                precos_escolhidos,
                rodadas_atletas,
                mando_flag=0,
            )

        st.dataframe(atletas_out_df)

    # Scouts Jogador
    st.subheader('Comparação de Scouts')
    atletas_ids = st.multiselect(
        'Selecione até 5 IDs de Jogadores',
        options=atletas_out_index2names.keys(),
        max_selections=5,
        key='id_jogador',
    )

    if len(atletas_ids) > 0:
        if media_opcao == 'Geral':
            st.plotly_chart(
                P.plot_player_scouts(
                    atletas_ids, atletas_out_index2names, rodadas_atletas
                ),
                use_container_width=True,
            )
        elif media_opcao == 'Mandante':
            st.plotly_chart(
                P.plot_player_scouts(
                    atletas_ids,
                    atletas_out_index2names,
                    rodadas_atletas,
                    atletas_df=atletas_df.set_index('atleta_id'),
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
                    atletas_df=atletas_df.set_index('atleta_id'),
                    mando_flag=0,
                ),
                use_container_width=True,
            )

    # Pontos Cedidos
    st.title('Pontos Cedidos')
    rodadas_pontos_cedidos = (1, 1)
    container_pontos_cedidos = st.container()

    if rodada_atual > 1:
        rodadas_pontos_cedidos = st.slider(
            'Selecione um intervalo de rodadas',
            RODADA_INICIAL,
            rodada_atual,
            (RODADA_INICIAL, rodada_atual),
            key='rodadas_pontos_cedidos',
        )

    posicao_escolhida = st.selectbox('Selecione uma Posição', posicoes_list)
    abreviacao2posicao = {
        'GOL': '1',
        'LAT': '2',
        'ZAG': '3',
        'MEI': '4',
        'ATA': '5',
        'TEC': '6',
    }
    pontos_cedidos_posicao = pd.read_csv(
        f'data/csv/pontos_cedidos/{abreviacao2posicao[posicao_escolhida]}.csv'
    ).set_index('clube_id')

    if media_opcao == 'Geral':
        with container_pontos_cedidos:
            st.write(
                'Quantos pontos um time cede em média para um atleta de uma determinada posição.'
            )

        st.dataframe(
            P.plot_pontos_cedidos_geral(pontos_cedidos_posicao, rodadas_pontos_cedidos)
        )
    elif media_opcao == 'Mandante':
        with container_pontos_cedidos:
            st.write(
                'Quantos pontos um time cede em média para um atleta que joga como mandante em uma determinada posição.'
            )

        st.dataframe(
            P.plot_pontos_cedidos_mando(
                pontos_cedidos_posicao, rodadas_pontos_cedidos, mando_flag=0
            )
        )
    else:
        with container_pontos_cedidos:
            st.write(
                'Quantos pontos um time cede em média para um atleta que joga como visitante em uma determinada posição.'
            )
        st.dataframe(
            P.plot_pontos_cedidos_mando(
                pontos_cedidos_posicao, rodadas_pontos_cedidos, mando_flag=1
            )
        )


if __name__ == '__main__':
    asyncio.run(main())
