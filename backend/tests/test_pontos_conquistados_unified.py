import pandas as pd
import pytest
from src.services.enums import Scout
from src.services.pontos_conquistados_unified import compute_pontos_conquistados_unified


class TestComputePontosConquistadosUnified:
    @pytest.fixture
    def sample_pontuacoes_df(self):
        return pd.DataFrame(
            {
                "atleta_id": [1, 2, 3, 4, 5, 6, 7, 8],
                "clube_id": [256, 256, 282, 282, 256, 256, 282, 282],
                "posicao_id": [1, 1, 1, 1, 2, 2, 2, 2],
                "is_mandante": [True, False, True, False, True, False, True, False],
                "rodada_id": [1, 1, 1, 1, 1, 1, 1, 1],
                "pontuacao": [5.0, 3.0, 4.0, 6.0, 2.0, 4.0, 3.0, 5.0],
                "pontuacao_basica": [4.0, 2.5, 3.5, 5.0, 1.5, 3.0, 2.5, 4.0],
                "status_id": [7, 7, 2, 2, 7, 7, 2, 2],
                **{
                    scout: [1 if i == 0 else 0 for i in range(8)]
                    for scout in Scout.as_list()
                },
            }
        )

    def test_returns_dataframe_with_correct_columns(self, sample_pontuacoes_df):
        result = compute_pontos_conquistados_unified(
            sample_pontuacoes_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        expected_columns = [
            "clube_id",
            "media_conquistada",
            "media_conquistada_basica",
            "total_jogos",
            "scouts",
        ]
        for col in expected_columns:
            assert col in result.columns

    def test_filters_by_posicao_id(self, sample_pontuacoes_df):
        result = compute_pontos_conquistados_unified(
            sample_pontuacoes_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        assert result["clube_id"].nunique() > 0

    def test_filters_by_rodada_range(self, sample_pontuacoes_df):
        result = compute_pontos_conquistados_unified(
            sample_pontuacoes_df,
            rodada_min=2,
            rodada_max=5,
            is_mandante="geral",
            posicao_id=1,
        )
        assert result.empty

    def test_is_mandante_geral_returns_all(self, sample_pontuacoes_df):
        result = compute_pontos_conquistados_unified(
            sample_pontuacoes_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        assert not result.empty
        assert len(result) == 2

    def test_is_mandante_filters_mandante(self, sample_pontuacoes_df):
        result = compute_pontos_conquistados_unified(
            sample_pontuacoes_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="mandante",
            posicao_id=1,
        )
        assert not result.empty
        for _, row in result.iterrows():
            pont_rows = sample_pontuacoes_df[
                (sample_pontuacoes_df["clube_id"] == row["clube_id"])
                & (sample_pontuacoes_df["posicao_id"] == 1)
                & (sample_pontuacoes_df["is_mandante"])
            ]
            assert len(pont_rows) > 0

    def test_is_mandante_filters_visitante(self, sample_pontuacoes_df):
        result = compute_pontos_conquistados_unified(
            sample_pontuacoes_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="visitante",
            posicao_id=1,
        )
        assert not result.empty
        for _, row in result.iterrows():
            pont_rows = sample_pontuacoes_df[
                (sample_pontuacoes_df["clube_id"] == row["clube_id"])
                & (sample_pontuacoes_df["posicao_id"] == 1)
                & (~sample_pontuacoes_df["is_mandante"])
            ]
            assert len(pont_rows) > 0

    def test_media_conquistada_is_mean(self, sample_pontuacoes_df):
        result = compute_pontos_conquistados_unified(
            sample_pontuacoes_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        clube_256 = result[result["clube_id"] == 256].iloc[0]
        expected_mean = (5.0 + 3.0) / 2
        assert abs(clube_256["media_conquistada"] - expected_mean) < 0.01

    def test_total_jogos_is_unique_rodadas(self, sample_pontuacoes_df):
        result = compute_pontos_conquistados_unified(
            sample_pontuacoes_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        clube_256 = result[result["clube_id"] == 256].iloc[0]
        assert clube_256["total_jogos"] == 1

    def test_scouts_is_dict(self, sample_pontuacoes_df):
        result = compute_pontos_conquistados_unified(
            sample_pontuacoes_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        assert result["scouts"].dtype == object
        assert isinstance(result.iloc[0]["scouts"], dict)

    def test_empty_dataframe_returns_empty(self, sample_pontuacoes_df):
        result = compute_pontos_conquistados_unified(
            sample_pontuacoes_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=999,
        )
        assert result.empty

    def test_media_conquistada_rounded_to_2_decimals(self, sample_pontuacoes_df):
        result = compute_pontos_conquistados_unified(
            sample_pontuacoes_df,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
        )
        for _, row in result.iterrows():
            assert row["media_conquistada"] == round(row["media_conquistada"], 2)
            assert row["media_conquistada_basica"] == round(
                row["media_conquistada_basica"], 2
            )


class TestComputePontosConquistadosUnifiedWithStatus:
    @pytest.fixture
    def pontuacoes_with_status(self):
        return pd.DataFrame(
            {
                "atleta_id": [1, 2, 3, 4],
                "clube_id": [256, 256, 282, 282],
                "posicao_id": [1, 1, 1, 1],
                "is_mandante": [True, True, True, True],
                "rodada_id": [1, 1, 1, 1],
                "pontuacao": [5.0, 3.0, 4.0, 6.0],
                "pontuacao_basica": [4.0, 2.5, 3.5, 5.0],
                "status_id": [7, 2, 7, 2],
                **{scout: [0] * 4 for scout in Scout.as_list()},
            }
        )

    def test_status_ids_none_returns_all(self, pontuacoes_with_status):
        result = compute_pontos_conquistados_unified(
            pontuacoes_with_status,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
            status_ids=None,
        )
        assert len(result) == 2

    def test_status_ids_empty_list_returns_all(self, pontuacoes_with_status):
        result = compute_pontos_conquistados_unified(
            pontuacoes_with_status,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
            status_ids=[],
        )
        assert len(result) == 2

    def test_filters_by_single_status_id(self, pontuacoes_with_status):
        result = compute_pontos_conquistados_unified(
            pontuacoes_with_status,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
            status_ids=[7],
        )
        assert len(result) == 2
        expected_mean = 5.0
        assert (
            abs(
                result[result["clube_id"] == 256]["media_conquistada"].iloc[0]
                - expected_mean
            )
            < 0.01
        )

    def test_filters_by_multiple_status_ids(self, pontuacoes_with_status):
        result = compute_pontos_conquistados_unified(
            pontuacoes_with_status,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
            status_ids=[7, 2],
        )
        assert len(result) == 2

    def test_status_filter_with_no_matches_returns_empty(self, pontuacoes_with_status):
        result = compute_pontos_conquistados_unified(
            pontuacoes_with_status,
            rodada_min=1,
            rodada_max=1,
            is_mandante="geral",
            posicao_id=1,
            status_ids=[999],
        )
        assert result.empty


class TestComputePontosConquistadosUnifiedDirectMando:
    @pytest.fixture
    def pontuacoes_direct_mando(self):
        return pd.DataFrame(
            {
                "clube_id": [256, 256, 282, 282],
                "posicao_id": [1, 1, 1, 1],
                "is_mandante": [True, False, True, False],
                "rodada_id": [1, 1, 1, 1],
                "pontuacao": [5.0, 3.0, 4.0, 6.0],
                "pontuacao_basica": [4.0, 2.5, 3.5, 5.0],
                "status_id": [7, 7, 7, 7],
                **{scout: [0] * 4 for scout in Scout.as_list()},
            }
        )

    def test_mandante_filter_shows_earned_at_home(self, pontuacoes_direct_mando):
        result = compute_pontos_conquistados_unified(
            pontuacoes_direct_mando,
            rodada_min=1,
            rodada_max=1,
            is_mandante="mandante",
            posicao_id=1,
        )
        for _, row in result.iterrows():
            pont_rows = pontuacoes_direct_mando[
                (pontuacoes_direct_mando["clube_id"] == row["clube_id"])
                & (pontuacoes_direct_mando["posicao_id"] == 1)
                & (pontuacoes_direct_mando["is_mandante"])
            ]
            assert len(pont_rows) > 0

    def test_visitante_filter_shows_earned_away(self, pontuacoes_direct_mando):
        result = compute_pontos_conquistados_unified(
            pontuacoes_direct_mando,
            rodada_min=1,
            rodada_max=1,
            is_mandante="visitante",
            posicao_id=1,
        )
        for _, row in result.iterrows():
            pont_rows = pontuacoes_direct_mando[
                (pontuacoes_direct_mando["clube_id"] == row["clube_id"])
                & (pontuacoes_direct_mando["posicao_id"] == 1)
                & (~pontuacoes_direct_mando["is_mandante"])
            ]
            assert len(pont_rows) > 0
