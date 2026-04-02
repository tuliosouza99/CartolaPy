import marimo

__generated_with = "0.22.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import aiohttp
    import pandas as pd
    from request_handler import RequestHandler
    from enums import Scout

    return RequestHandler, Scout, pd


@app.cell
def _():
    rodada = 3
    return (rodada,)


@app.cell
async def _(RequestHandler, Scout, pd, rodada):
    pontuacoes_handler = RequestHandler()
    pontuacoes_columns = [
        "atleta_id",
        "posicao_id",
        "clube_id",
        "rodada_id",
        "pontuacao",
        "pontuacao_basica",
        *Scout.as_list(),
    ]
    pontuacoes_json = await pontuacoes_handler.make_get_request(
        f"https://api.cartola.globo.com/atletas/pontuados/{rodada}"
    )
    raw_pontuacoes_df = (
        pd.DataFrame(pontuacoes_json["atletas"])
        .T.reset_index(names="atleta_id")
        .loc[lambda df_: df_["entrou_em_campo"]]
    )
    normalized_pontuacoes_df = raw_pontuacoes_df.join(pd.json_normalize(raw_pontuacoes_df["scout"])).fillna(0)

    pontuacoes_df = (
        normalized_pontuacoes_df.assign(
            **{k: 0 for k in Scout.as_list() if k not in normalized_pontuacoes_df.columns}
        )
        .astype({k: "int64" for k in Scout.as_list()})
        .assign(
            rodada_id=rodada,
            pontuacao_basica=lambda df_: (
                df_[Scout.as_basic_scouts_list()]
                .mul(
                    {
                        k: getattr(Scout, k).value["value"]
                        for k in Scout.as_basic_scouts_list()
                    }
                )
                .sum(axis=1)
            ),
        )
        .loc[:, pontuacoes_columns]
    )

    pontuacoes_df
    return (pontuacoes_df,)


@app.cell
async def _(RequestHandler, pd, rodada):
    confrontos_handler = RequestHandler()
    confrontos_columns = ["clube_id", "opponent_clube_id", "is_mandante", "rodada_id"]
    confrontos_json = await confrontos_handler.make_get_request(
        f"https://api.cartola.globo.com/partidas/{rodada}"
    )
    raw_confrontos_df = pd.DataFrame(confrontos_json["partidas"]).loc[lambda df_: df_["valida"]]

    confrontos_df = pd.concat(
        [
            (
                raw_confrontos_df.rename(
                    columns={
                        "clube_casa_id": "clube_id",
                        "clube_visitante_id": "opponent_clube_id",
                    }
                )
                .assign(rodada_id=rodada, is_mandante=True)
                .loc[:, confrontos_columns]
            ),
            (
                raw_confrontos_df.rename(
                    columns={
                        "clube_visitante_id": "clube_id",
                        "clube_casa_id": "opponent_clube_id",
                    }
                )
                .assign(rodada_id=rodada, is_mandante=False)
                .loc[:, confrontos_columns]
            ),
        ],
        ignore_index=True,
    )

    confrontos_df
    return (confrontos_df,)


@app.cell
def _(Scout, confrontos_df, pontuacoes_df):
    pontuacoes_agg = pontuacoes_df.groupby(
        ["clube_id", "posicao_id", "rodada_id"], as_index=False
    ).agg(
        {col: "mean" for col in ["pontuacao", "pontuacao_basica", *Scout.as_list()]}
    )

    positions_df = pontuacoes_df[["posicao_id"]].drop_duplicates()

    pontos_cedidos_df = (
        confrontos_df.merge(positions_df, how="cross")
        .merge(
            pontuacoes_agg.rename(columns={"clube_id": "opponent_clube_id"}),
            on=["opponent_clube_id", "posicao_id", "rodada_id"],
        )
        .drop(columns=["opponent_clube_id"])
    )

    pontos_cedidos_df
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
