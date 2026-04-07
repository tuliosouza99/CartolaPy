import pandas as pd
import pytest
from src.services.data_loaders.pontos_cedidos import PontosCedidos
from src.services.enums import Scout


class TestPontosCedidos:
    @pytest.fixture
    def pontos_cedidos(self):
        return PontosCedidos()

    @pytest.fixture
    def sample_pontuacoes_df(self):
        return pd.DataFrame(
            {
                "atleta_id": [1, 2, 3, 4],
                "posicao_id": [1, 1, 2, 2],
                "clube_id": [264, 276, 264, 276],
                "rodada_id": [1, 1, 1, 1],
                "pontuacao": [8.0, 5.0, 6.0, 4.0],
                "pontuacao_basica": [8.0, 5.0, 6.0, 4.0],
                **{
                    scout: [1 if scout == "G" else 0 for _ in range(4)]
                    for scout in Scout.as_list()
                },
            }
        )

    @pytest.fixture
    def sample_confrontos_df(self):
        return pd.DataFrame(
            {
                "clube_id": [264, 264, 276, 276],
                "opponent_clube_id": [276, 303, 264, 287],
                "is_mandante": [True, True, False, False],
                "rodada_id": [1, 2, 1, 2],
                "partida_id": [1001, 1002, 1001, 1002],
            }
        )

    def test_initial_df_is_empty_with_correct_columns(self, pontos_cedidos):
        assert isinstance(pontos_cedidos.df, pd.DataFrame)
        assert pontos_cedidos.df.empty
        expected_columns = [
            "clube_id",
            "posicao_id",
            "is_mandante",
            "rodada_id",
            "partida_id",
            "pontuacao",
            "pontuacao_basica",
            *Scout.as_list(),
        ]
        assert list(pontos_cedidos.df.columns) == expected_columns

    def test_fill_pontos_cedidos_aggregates_by_opponent(
        self, pontos_cedidos, sample_pontuacoes_df, sample_confrontos_df
    ):
        sample_pontuacoes_df = sample_pontuacoes_df.copy()
        sample_pontuacoes_df["G"] = [1, 0, 0, 0]

        pontos_cedidos.fill_pontos_cedidos(sample_pontuacoes_df, sample_confrontos_df)

        assert not pontos_cedidos.df.empty
        assert "clube_id" in pontos_cedidos.df.columns
        assert "opponent_clube_id" not in pontos_cedidos.df.columns

    def test_fill_pontos_cedidos_creates_cross_join_with_positions(
        self, pontos_cedidos, sample_pontuacoes_df, sample_confrontos_df
    ):
        pontos_cedidos.fill_pontos_cedidos(sample_pontuacoes_df, sample_confrontos_df)

        assert not pontos_cedidos.df.empty
        assert "posicao_id" in pontos_cedidos.df.columns
        assert "clube_id" in pontos_cedidos.df.columns
        assert pontos_cedidos.df["posicao_id"].nunique() > 0

    def test_fill_pontos_cedidos_includes_is_mandante(
        self, pontos_cedidos, sample_pontuacoes_df, sample_confrontos_df
    ):
        pontos_cedidos.fill_pontos_cedidos(sample_pontuacoes_df, sample_confrontos_df)

        assert "is_mandante" in pontos_cedidos.df.columns
        assert pontos_cedidos.df["is_mandante"].isin([True, False]).all()

    def test_fill_pontos_cedidos_includes_rodada_id(
        self, pontos_cedidos, sample_pontuacoes_df, sample_confrontos_df
    ):
        pontos_cedidos.fill_pontos_cedidos(sample_pontuacoes_df, sample_confrontos_df)

        assert "rodada_id" in pontos_cedidos.df.columns
        assert pontos_cedidos.df["rodada_id"].nunique() > 0

    def test_fill_pontos_cedidos_has_all_scout_columns(
        self, pontos_cedidos, sample_pontuacoes_df, sample_confrontos_df
    ):
        pontos_cedidos.fill_pontos_cedidos(sample_pontuacoes_df, sample_confrontos_df)

        for scout_name in Scout.as_list():
            assert scout_name in pontos_cedidos.df.columns

    def test_fill_pontos_cedidos_aggregates_pontuacao(
        self, pontos_cedidos, sample_pontuacoes_df, sample_confrontos_df
    ):
        pontos_cedidos.fill_pontos_cedidos(sample_pontuacoes_df, sample_confrontos_df)

        assert "pontuacao" in pontos_cedidos.df.columns
        assert pontos_cedidos.df["pontuacao"].notna().any()

    def test_df_property_returns_dataframe(self, pontos_cedidos):
        assert isinstance(pontos_cedidos.df, pd.DataFrame)

    def test_fill_pontos_cedidos_empty_pontuacoes(
        self, pontos_cedidos, sample_confrontos_df
    ):
        empty_pontuacoes = pd.DataFrame(
            columns=[
                "atleta_id",
                "posicao_id",
                "clube_id",
                "rodada_id",
                "pontuacao",
                "pontuacao_basica",
                *Scout.as_list(),
            ]
        )

        pontos_cedidos.fill_pontos_cedidos(empty_pontuacoes, sample_confrontos_df)

        assert pontos_cedidos.df.empty

    def test_fill_pontos_cedidos_empty_confrontos(
        self, pontos_cedidos, sample_pontuacoes_df
    ):
        empty_confrontos = pd.DataFrame(
            columns=["clube_id", "opponent_clube_id", "is_mandante", "rodada_id"]
        )

        pontos_cedidos.fill_pontos_cedidos(sample_pontuacoes_df, empty_confrontos)

        assert pontos_cedidos.df.empty

    def test_fill_pontos_cedidos_computes_mean_not_sum(self, pontos_cedidos):
        pontuacoes = pd.DataFrame(
            {
                "atleta_id": [1, 2, 3],
                "posicao_id": [1, 1, 1],
                "clube_id": [282, 282, 282],
                "rodada_id": [1, 1, 1],
                "pontuacao": [4.0, 6.0, 10.0],
                "pontuacao_basica": [3.0, 5.0, 8.0],
                **{
                    scout: [1 if scout == "G" else 0 for _ in range(3)]
                    for scout in Scout.as_list()
                },
            }
        )
        confrontos = pd.DataFrame(
            {
                "clube_id": [256],
                "opponent_clube_id": [282],
                "is_mandante": [True],
                "rodada_id": [1],
            }
        )

        pontos_cedidos.fill_pontos_cedidos(pontuacoes, confrontos)

        result = pontos_cedidos.df[pontos_cedidos.df["clube_id"] == 256].iloc[0]
        expected_mean = (4.0 + 6.0 + 10.0) / 3
        assert abs(result["pontuacao"] - expected_mean) < 0.01
        assert abs(result["G"] - 1.0) < 0.01
