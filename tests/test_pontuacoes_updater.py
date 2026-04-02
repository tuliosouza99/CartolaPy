import pandas as pd
import numpy as np

from src.pontuacoes_updater import create_base_df


class TestCreateBaseDf:
    def test_create_base_df_has_correct_columns(self):
        atletas_df = pd.DataFrame({"atleta_id": [1, 2, 3]})
        result = create_base_df(atletas_df)
        assert list(result.columns) == ["atleta_id", "rodada"]

    def test_create_base_df_has_38_rows_per_atleta(self):
        atletas_df = pd.DataFrame({"atleta_id": [1, 2]})
        result = create_base_df(atletas_df)
        assert len(result) == 76

    def test_create_base_df_rodada_values(self):
        atletas_df = pd.DataFrame({"atleta_id": [1]})
        result = create_base_df(atletas_df)
        assert list(result["rodada"]) == list(range(1, 39))
