# %%
import os 
import pandas as pd

# %%
dominio = "www.senamhi.gob.pe"
subfolder= "mapas/mapa-estaciones-2"
resource = "_dato_esta_tipo02.php"
station_id = "47E86310"
month= "202204"
parameters = f"estaciones={station_id}&CBOFiltro={month}&t_e=M&estado=AUTOMATICA&cod_old=&cate_esta=EMA&alt=165"
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

# %%
columns=["date","hour","temperature","precipitation","humidity","wind_direction","wind_velocity"]
df_table = df_table.iloc[1:].set_axis(columns,axis=1)
# %%
#os.makedirs("data", exist_ok=True)
#df_table.to_csv(f"data/{station_id}-{month}.csv", index=False)

# %%
DATASET = "raw_zone"
PROJECT_ID = "focus-infusion-348919"
table_name = "senami_data"
table_schema=[
        {"name":"date","type":"STRING"},
        {"name":"hour","type":"STRING"},
        {"name":"temperature","type":"NUMERIC"},
        {"name":"precipitation","type":"NUMERIC"},
        {"name":"humidity","type":"NUMERIC"},
        {"name":"wind_direction","type":"NUMERIC"},
        {"name":"wind_velocity","type":"NUMERIC"}
    ]
df_table.to_gbq(f"{DATASET}.{table_name}",project_id=PROJECT_ID,
        if_exists="append",
        table_schema=table_schema)
# %%
