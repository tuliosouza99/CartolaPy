import pandas as pd
import pytest

from backend.services.enums import Scout
from backend.services.pontos_cedidos_unified import compute_pontos_cedidos_unified


class TestComputePontosCedidosUnified:
    @pytest.fixture
    def sample_pontos_cedidos_df(self):
        return pd.DataFrame(
            {
                "clube_id": [256, 256, 282, 282, 256, 256, 282, 282],
                "posicao_id": [1, 1, 1, 1, 2, 2, 2, 2],
                "is_mandante": [True, False, True, False, True, False, True, False],
                "rodada_id": [1, 1, 1, 1, 1, 1, 1, 1],
                "pontuacao": [5.0, 3.0, 4.0, 6.0, 2.0, 4.0, 3.0, 5.0],
                "pontuacao_basica": [4.0, 2.5, 3.5, 5.0, 1.5, 3.0, 2.5, 4.0],
                **{
                    scout: [1 if i == 0 else 0 for i in range(8)]
                    for scout in Scout.as_list()
                },
            }
        )

    def test_returns_dataframe_with_correct_columns(self, sample_pontos_cedidos_df):
        result = compute_pontos_cedidos_unified(
            sample_pontos_cedidos_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        expected_columns = [
            "clube_id",
            "media_cedida",
            "media_cedida_basica",
            "total_jogos",
            "scouts",
        ]
        for col in expected_columns:
            assert col in result.columns

    def test_filters_by_posicao_id(self, sample_pontos_cedidos_df):
        result = compute_pontos_cedidos_unified(
            sample_pontos_cedidos_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        assert result["clube_id"].nunique() > 0

    def test_filters_by_rodada_range(self, sample_pontos_cedidos_df):
        result = compute_pontos_cedidos_unified(
            sample_pontos_cedidos_df,
            rodada_min=2,
            rodada_max=5,
            is_mandante="geral",
            posicao_id=1,
        )
        assert result.empty

    def test_is_mandante_geral_returns_all(self, sample_pontos_cedidos_df):
        result = compute_pontos_cedidos_unified(
            sample_pontos_cedidos_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        assert not result.empty
        assert len(result) == 2

    def test_is_mandante_filters_mandante(self, sample_pontos_cedidos_df):
        result = compute_pontos_cedidos_unified(
            sample_pontos_cedidos_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="mandante",
            posicao_id=1,
        )
        assert not result.empty
        for _, row in result.iterrows():
            pc_rows = sample_pontos_cedidos_df[
                (sample_pontos_cedidos_df["clube_id"] == row["clube_id"])
                & (sample_pontos_cedidos_df["posicao_id"] == 1)
                & (sample_pontos_cedidos_df["is_mandante"])
            ]
            assert len(pc_rows) > 0

    def test_is_mandante_filters_visitante(self, sample_pontos_cedidos_df):
        result = compute_pontos_cedidos_unified(
            sample_pontos_cedidos_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="visitante",
            posicao_id=1,
        )
        assert not result.empty
        for _, row in result.iterrows():
            pc_rows = sample_pontos_cedidos_df[
                (sample_pontos_cedidos_df["clube_id"] == row["clube_id"])
                & (sample_pontos_cedidos_df["posicao_id"] == 1)
                & (~sample_pontos_cedidos_df["is_mandante"])
            ]
            assert len(pc_rows) > 0

    def test_media_cedida_is_mean(self, sample_pontos_cedidos_df):
        result = compute_pontos_cedidos_unified(
            sample_pontos_cedidos_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        clube_256 = result[result["clube_id"] == 256].iloc[0]
        expected_mean = (5.0 + 3.0) / 2
        assert abs(clube_256["media_cedida"] - expected_mean) < 0.01

    def test_total_jogos_is_unique_rodadas(self, sample_pontos_cedidos_df):
        result = compute_pontos_cedidos_unified(
            sample_pontos_cedidos_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        clube_256 = result[result["clube_id"] == 256].iloc[0]
        assert clube_256["total_jogos"] == 1

    def test_scouts_is_dict(self, sample_pontos_cedidos_df):
        result = compute_pontos_cedidos_unified(
            sample_pontos_cedidos_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        assert result["scouts"].dtype == object
        assert isinstance(result.iloc[0]["scouts"], dict)

    def test_empty_dataframe_returns_empty(self, sample_pontos_cedidos_df):
        result = compute_pontos_cedidos_unified(
            sample_pontos_cedidos_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=999,
        )
        assert result.empty

    def test_media_cedida_rounded_to_2_decimals(self, sample_pontos_cedidos_df):
        result = compute_pontos_cedidos_unified(
            sample_pontos_cedidos_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        for _, row in result.iterrows():
            assert row["media_cedida"] == round(row["media_cedida"], 2)
            assert row["media_cedida_basica"] == round(row["media_cedida_basica"], 2)


class TestComputePontosCedidosUnifiedInvertedMando:
    @pytest.fixture
    def pontos_cedidos_inverted_mando(self):
        return pd.DataFrame(
            {
                "clube_id": [256, 256, 282, 282],
                "posicao_id": [1, 1, 1, 1],
                "is_mandante": [True, False, True, False],
                "rodada_id": [1, 1, 1, 1],
                "pontuacao": [5.0, 3.0, 4.0, 6.0],
                "pontuacao_basica": [4.0, 2.5, 3.5, 5.0],
                **{scout: [0] * 4 for scout in Scout.as_list()},
            }
        )

    def test_mandante_filter_shows_conceded_to_home_opponents(
        self, pontos_cedidos_inverted_mando
    ):
        result = compute_pontos_cedidos_unified(
            pontos_cedidos_inverted_mando,
            rodada_min=1,
            rodada_max=1,
            is_mandante="mandante",
            posicao_id=1,
        )
        for _, row in result.iterrows():
            assert row["clube_id"] in [256, 282]

    def test_visitante_filter_shows_conceded_to_away_opponents(
        self, pontos_cedidos_inverted_mando
    ):
        result = compute_pontos_cedidos_unified(
            pontos_cedidos_inverted_mando,
            rodada_min=1,
            rodada_max=1,
            is_mandante="visitante",
            posicao_id=1,
        )
        for _, row in result.iterrows():
            assert row["clube_id"] in [256, 282]
