python src/pre_season/create_dicts.py
python src/pre_season/create_dfs.py --create_mandos_and_confrontos --create_pontos_cedidos
python src/update_atletas.py
python src/update_pontuacoes.py

# Can also set specific rounds in line 8 with --rounds x y z.
# In this case, remove --all_rounds in line 8 and --create_mandos_and_confrontos in line 2
python src/update_confrontos_or_mandos.py --all_rounds

# Can also set specific rounds in line 12 with --rounds x y z.
# In this case, remove --all_rounds in line 12 and --create_pontos_cedidos in line 2
python src/update_pontos_cedidos.py --all_rounds