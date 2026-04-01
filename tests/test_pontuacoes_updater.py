import pytest
import pandas as pd
import numpy as np

from src.pontuacoes_updater import create_df


class TestCreateDf:
    def test_create_df_has_correct_number_of_rows(self):
        atletas_df = pd.DataFrame(
            {
                "atleta_id": [1, 2, 3],
            }
        )
        result = create_df(atletas_df)
        assert len(result) == 3

    def test_create_df_has_38_round_columns(self):
        atletas_df = pd.DataFrame(
            {
                "atleta_id": [1, 2],
            }
        )
        result = create_df(atletas_df)
        columns = [col for col in result.columns if col != "atleta_id"]
        assert len(columns) == 38
        assert columns == [str(i) for i in range(1, 39)]

    def test_create_df_uses_atleta_id_as_index(self):
        atletas_df = pd.DataFrame(
            {
                "atleta_id": [10, 20, 30],
            }
        )
        result = create_df(atletas_df)
        assert list(result["atleta_id"]) == [10, 20, 30]

    def test_create_df_all_values_are_nan(self):
        atletas_df = pd.DataFrame(
            {
                "atleta_id": [1, 2],
            }
        )
        result = create_df(atletas_df)
        data_cols = [col for col in result.columns if col != "atleta_id"]
        for col in data_cols:
            assert all(np.isnan(result[col]))
