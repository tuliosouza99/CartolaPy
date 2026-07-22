import pandas as pd
from src.services.player_view import build_cartola_player_view


def test_cartola_player_view_filters_venue_and_keeps_source_separate():
    player = {
        "atleta_id": 1,
        "clube_id": 10,
        "posicao_id": 4,
        "status_id": 7,
        "apelido": "Victor Hugo",
        "nome": "Victor Hugo Gomes Silva",
        "foto": "photo.png",
        "preco_num": 12.4,
    }
    pontuacoes = pd.DataFrame(
        [
            {
                "atleta_id": 1,
                "clube_id": 10,
                "rodada_id": 1,
                "pontuacao": 8.0,
                "pontuacao_basica": 5.0,
                "G": 1,
            },
            {
                "atleta_id": 1,
                "clube_id": 10,
                "rodada_id": 2,
                "pontuacao": 2.0,
                "pontuacao_basica": 2.0,
                "G": 0,
            },
        ]
    )
    confrontos = pd.DataFrame(
        [
            {
                "clube_id": 10,
                "rodada_id": 1,
                "partida_id": 100,
                "opponent_clube_id": 20,
                "is_mandante": True,
            },
            {
                "clube_id": 10,
                "rodada_id": 2,
                "partida_id": 101,
                "opponent_clube_id": 30,
                "is_mandante": False,
            },
        ]
    )

    result = build_cartola_player_view(
        player,
        pontuacoes,
        confrontos,
        {"10": {"nome": "Clube A"}, "20": {"nome": "Clube B"}},
        {"4": {"nome": "Meia", "abreviacao": "mei"}},
        {"7": {"nome": "Provável"}},
        1,
        2,
        "mandante",
    )

    assert result["data_provider"] == "Cartola FC"
    assert result["summary"]["matches"] == 1
    assert result["summary"]["average_points"] == 8.0
    assert result["summary"]["scouts"] == {"G": 1}
    assert result["matches"][0]["opponent_name"] == "Clube B"
    assert result["matches"][0]["scouts"] == {"G": 1}
