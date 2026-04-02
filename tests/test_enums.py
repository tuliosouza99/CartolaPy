from backend.services.enums import Scout


class TestScoutEnum:
    def test_as_list_returns_all_scout_names(self):
        result = Scout.as_list()
        expected = [
            "G",
            "A",
            "FT",
            "FD",
            "FF",
            "FS",
            "PS",
            "V",
            "I",
            "PP",
            "DS",
            "SG",
            "DE",
            "DP",
            "CV",
            "CA",
            "FC",
            "GC",
            "GS",
            "PC",
        ]
        assert len(result) == 20
        assert result == expected

    def test_as_basic_scouts_list_excludes_special_scouts(self):
        result = Scout.as_basic_scouts_list()
        excluded = {"G", "A", "FT", "PP", "DP", "SG", "CV", "GC"}
        assert all(scout not in excluded for scout in result)

    def test_as_basic_scouts_list_returns_expected_count(self):
        result = Scout.as_basic_scouts_list()
        assert len(result) == 12

    def test_as_basic_scouts_list_contains_expected_scouts(self):
        result = Scout.as_basic_scouts_list()
        expected_basic = {
            "FD",
            "FF",
            "FS",
            "PS",
            "V",
            "I",
            "DS",
            "DE",
            "CA",
            "FC",
            "GS",
            "PC",
        }
        assert set(result) == expected_basic

    def test_gol_value_is_8(self):
        assert Scout.G.value["value"] == 8

    def test_assistencia_value_is_5(self):
        assert Scout.A.value["value"] == 5

    def test_penalty_perdido_value_is_negative_4(self):
        assert Scout.PP.value["value"] == -4

    def test_gol_contra_value_is_negative_3(self):
        assert Scout.GC.value["value"] == -3

    def test_scout_names_are_unique(self):
        names = [scout.name for scout in Scout]
        assert len(names) == len(set(names))

    def test_scout_values_are_dicts_with_name_and_value(self):
        for scout in Scout:
            assert isinstance(scout.value, dict)
            assert "name" in scout.value
            assert "value" in scout.value
