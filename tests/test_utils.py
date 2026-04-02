import pandas as pd
import numpy as np

from src.utils import (
    create_mando_dict,
    get_basic_points,
    color_status,
)
from src.enums import Scout


class TestCreateMandoDict:
    def test_create_mando_dict_home(self, sample_mandos_df):
        result = create_mando_dict(sample_mandos_df, mando_flag=1)
        assert result[1] == [1, 3, 5]
        assert result[2] == [2, 4]
        assert result[3] == [1]

    def test_create_mando_dict_away(self, sample_mandos_df):
        result = create_mando_dict(sample_mandos_df, mando_flag=0)
        assert result[3] == [2]

    def test_create_mando_dict_empty_df(self):
        df = pd.DataFrame({"clube_id": [], "rodada": [], "mando": []})
        result = create_mando_dict(df, mando_flag=1)
        assert result == {}


class TestGetBasicPoints:
    def test_get_basic_points_with_valid_scouts(self, sample_scouts_dict):
        result = get_basic_points(sample_scouts_dict)
        ds_value = Scout.DS.value["value"]
        fs_value = Scout.FS.value["value"]
        ff_value = Scout.FF.value["value"]
        fd_value = Scout.FD.value["value"]
        expected = (3 * ds_value) + (2 * fs_value) + (1 * ff_value) + (2 * fd_value)
        assert result == expected

    def test_get_basic_points_none(self):
        assert np.isnan(get_basic_points(None))

    def test_get_basic_points_nan(self):
        assert np.isnan(get_basic_points(np.nan))

    def test_get_basic_points_float_nan(self):
        assert np.isnan(get_basic_points(float("nan")))

    def test_get_basic_points_empty_dict(self):
        result = get_basic_points({})
        assert result == 0

    def test_get_basic_points_only_excluded_scouts(self):
        scouts = {"G": 1, "A": 2}
        result = get_basic_points(scouts)
        assert result == 0


class TestColorStatus:
    def test_color_status_probavel(self):
        result = color_status("Provável")
        assert result == "color: limegreen"

    def test_color_status_duvida(self):
        result = color_status("Dúvida")
        assert result == "color: gold"

    def test_color_status_outros(self):
        result = color_status("Algum outro status")
        assert result == "color: indianred"

    def test_color_status_case_sensitive(self):
        assert color_status("provável") == "color: indianred"
        assert color_status("DÚVIDA") == "color: indianred"
