# %%
#import os 
import pandas as pd
# %%
dominio = "www.senamhi.gob.pe"
subfolder= "mapas/mapa-estaciones-2"
resource = "_dato_esta_tipo02.php"
station_id = "105107"
alt="722"
month= "201701" # desde 201701 hasta 202206
estado="REAL"
cate_esta="CP"
parameters = f"estaciones={station_id}&CBOFiltro={month}&t_e=M&estado={estado}&cod_old=&cate_esta={cate_esta}&alt={alt}"
url = f"https://{dominio}/{subfolder}/{resource}?{parameters}"
print(url)
tables = pd.read_html(url) #see examples here: https://pbpython.com/pandas-html-table.html
# %%
"""
Relevant links:
for renaming columns : https://note.nkmk.me/en/python-pandas-dataframe-rename/
"""
df_metadata = tables[0]
df_table = tables[1]
df_metadata = df_metadata.iloc[1:] # removes first row
list_df = [df_metadata.iloc[:,0:2],
            df_metadata.iloc[:,2:4],
            df_metadata.iloc[:,4:6].iloc[:-1]]
list_df = [ df.set_axis(['key', 'value'], axis=1) for df in list_df]
df_metadata = pd.concat(list_df, axis=0)
df_metadata["key"] = df_metadata["key"].str.replace("\xa0:","") #remove space(\xa0) and colon(:)

#%%
#df_metadata.reset_index(drop=True).to_dict(orient="records")

# %%
df_table = df_table.iloc[1: , :]
df_table
# %%
columns=[
    "date",
    "temperature_max",
    "temperature_min",
    "humidity",
    "precipitation"]
df_table = df_table.iloc[1:].set_axis(columns,axis=1)#.map({'S/D': 0})
for col in columns[1:]:
    df_table[col] = df_table[col].replace('S/D', 0).replace('T', 0).astype(float)
df_table["station_id"] = station_id

df_table = df_table[["station_id"] + columns]
# %%
#df_table.dtypes
df_table
# %%
#os.makedirs("data", exist_ok=True)
#df_table.to_csv(f"data/{station_id}-{month}.csv", index=False)
# %%
DATASET = "raw_zone"
PROJECT_ID = "proyecto-bigdata-318002"
table_name = "estacion_convencional"
table_schema=[
        {"name":"station_id","type":"STRING"},
        {"name":"date","type":"STRING"},
        {"name":"temperature_max","type":"FLOAT"},
        {"name":"temperature_min","type":"FLOAT"},
        {"name":"humidity","type":"FLOAT"},
        {"name":"precipitation","type":"FLOAT"},
    ]
df_table.to_gbq(f"{DATASET}.{table_name}",project_id=PROJECT_ID,
        if_exists="append",
        table_schema=table_schema)
# %%
