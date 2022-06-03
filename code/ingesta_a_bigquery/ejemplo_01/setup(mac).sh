# paso 1: crear environment

current_directory="./code/ingesta_a_bigquery/ejemplo_01"
(cd $current_directory && python3.8 -m venv .venv)

# paso 2: activar environment

(cd $current_directory && source .venv/bin/activate)

# paso 3: instalar librerias

(cd $current_directory && pip install -r requirements.txt)

# paso 4: setear interprete in visual studio

1.En tu VS code presionar Ctrl + Shift + P, para abrir la paleta de comandos.
2. Tipea y selecciona “Python: Select Interpreter”.
3. Elige tu environment de la lista.

# paso 5 run python
