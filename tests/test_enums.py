import pytest
from src.enums import Scout, DataPath


class TestScout:
    def test_as_basic_scouts_list_excludes_certain_scouts(self):
        basic_scouts = Scout.as_basic_scouts_list()
        excluded = {"G", "A", "FT", "PP", "DP", "SG", "CV", "GC"}
        for scout in excluded:
            assert scout not in basic_scouts

    def test_as_basic_scouts_list_includes_other_scouts(self):
        basic_scouts = Scout.as_basic_scouts_list()
        assert "DS" in basic_scouts
        assert "FS" in basic_scouts
        assert "FF" in basic_scouts
        assert "FD" in basic_scouts
        assert "FC" in basic_scouts
        assert "CA" in basic_scouts

    def test_scout_values(self):
        assert Scout.G.value == {"name": "Gol", "value": 8}
        assert Scout.DS.value == {"name": "Desarme", "value": 1.2}
        assert Scout.CA.value == {"name": "Cartão amarelo", "value": -1}


class TestDataPath:
    def test_as_list_returns_all_paths(self):
        paths = DataPath.as_list()
        assert len(paths) == 16
        assert "data/csv/atletas.csv" in paths
        assert "data/json/clubes.json" in paths
        assert "data/parquet/scouts.parquet" in paths

    def test_all_enum_members_have_values(self):
        for path in DataPath:
            assert path.value is not None
            assert len(path.value) > 0
