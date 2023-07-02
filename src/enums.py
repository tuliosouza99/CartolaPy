from enum import Enum


class Scout(Enum):
    G = {'name': 'Gol', 'value': 8}
    A = {'name': 'Assistência', 'value': 5}
    FT = {'name': 'Finalização na trave', 'value': 3}
    FD = {'name': 'Finalização defendida', 'value': 1.2}
    FF = {'name': 'Finalização pra fora', 'value': 0.8}
    FS = {'name': 'Falta sofrida', 'value': 0.5}
    PS = {'name': 'Pênalti sofrido', 'value': 1}
    V = {'name': 'Vitória', 'value': 1}
    I = {'name': 'Impedimento', 'value': -0.1}
    PP = {'name': 'Pênalti perdido', 'value': -4}
    DS = {'name': 'Desarme', 'value': 1.2}
    SG = {'name': 'Jogo sem sofrer gol', 'value': 5}
    DE = {'name': 'Defesa', 'value': 1}
    DP = {'name': 'Defesa de pênalti', 'value': 7}
    CV = {'name': 'Cartão vermelho', 'value': -3}
    CA = {'name': 'Cartão amarelo', 'value': -1}
    FC = {'name': 'Falta cometida', 'value': -0.3}
    GC = {'name': 'Gol contra', 'value': -3}
    GS = {'name': 'Gol sofrido', 'value': -1}
    PC = {'name': 'Pênalti cometido', 'value': -1}

    @classmethod
    def as_basic_scouts_list(cls):
        return [
            scout.name
            for scout in cls
            if scout.name not in ('G', 'A', 'FT', 'PP', 'DP', 'SG', 'CV', 'GC')
        ]


class DataPath(Enum):
    ATLETAS = 'data/csv/atletas.csv'
    CLUBES = 'data/csv/clubes.csv'
    CONFRONTOS = 'data/csv/confrontos.csv'
    MANDOS = 'data/csv/mandos.csv'
    PONTUACOES = 'data/csv/pontuacoes.csv'
    POSICOES = 'data/csv/posicoes.csv'
    PONTOS_CEDIDOS_GOL = 'data/csv/pontos_cedidos/1.csv'
    PONTOS_CEDIDOS_LAT = 'data/csv/pontos_cedidos/2.csv'
    PONTOS_CEDIDOS_ZAG = 'data/csv/pontos_cedidos/3.csv'
    PONTOS_CEDIDOS_MEI = 'data/csv/pontos_cedidos/4.csv'
    PONTOS_CEDIDOS_ATA = 'data/csv/pontos_cedidos/5.csv'
    PONTOS_CEDIDOS_TEC = 'data/csv/pontos_cedidos/6.csv'
    CLUBES_DICT = 'data/json/clubes.json'
    POSICOES_DICT = 'data/json/posicoes.json'
    STATUS_DICT = 'data/json/status.json'
    SCOUTS = 'data/parquet/scouts.parquet'

    @classmethod
    def as_list(cls):
        return [path.value for path in cls]


class UpdateTablesMsg(Enum):
    MERCADO_FECHADO = (
        'O mercado está fechado! Espere sua reabertura para atualizar as tabelas.'
    )
    NOT_ALL_TABLES_FOUND = (
        'Não encontramos todas as tabelas necessárias para a execução do aplicativo.'
    )
    SEASON_NOT_STARTED = (
        'A temporada ainda não começou! Espere os resultados da primeira rodada para usar o app.\n'
        'Enquanto isso, você pode atualizar as tabelas para a nova temporada, caso ainda não tenha feito.'
    )
    SUCCESS = 'Tabelas atualizadas com sucesso!'
