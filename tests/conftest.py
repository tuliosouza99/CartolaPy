import pytest
import pandas as pd


@pytest.fixture
def sample_mandos_df():
    columns = [str(i) for i in range(1, 6)]
    data = [
        [1, 0, 1, 0, 1],
        [0, 1, 0, 1, 0],
        [1, 1, 0, 0, 1],
    ]
    index = [1, 2, 3]
    return pd.DataFrame(data, index=index, columns=columns)


@pytest.fixture
def sample_atletas_df():
    return pd.DataFrame(
        {
            "atleta_id": [1, 2, 3, 4],
            "apelido": ["Player A", "Player B", "Player C", "Player D"],
            "clube_id": [1, 1, 2, 2],
            "posicao_id": [1, 2, 1, 3],
            "status_id": [1, 2, 1, 3],
            "preco_num": [10.0, 20.0, 30.0, 5.0],
            "Média": [5.0, 6.0, 7.0, 2.0],
            "Média Básica": [4.0, 5.0, 6.0, 1.0],
            "Desvio Padrão": [1.0, 1.5, 2.0, 0.5],
            "Jogos": [10, 15, 20, 5],
        }
    )


@pytest.fixture
def sample_scouts_dict():
    return {
        "DS": 3,
        "FS": 2,
        "FF": 1,
        "FD": 2,
        "G": 1,
        "A": 1,
    }
