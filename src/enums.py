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
        return [scout.name for scout in cls if scout.name not in ('G', 'A', 'FT', 'PP', 'DP', 'SG', 'CV', 'GC')]
