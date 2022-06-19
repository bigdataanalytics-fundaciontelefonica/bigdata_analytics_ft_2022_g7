# %%
import pandas as pd
# %%
DATASET = "raw_zone"
PROJECT_ID = "proyecto-bigdata-318002"
TABLE_NAME = "estacion_convencional"


stations = [{"station_id":"104084","alt":"218"},
            {"station_id":"106014","alt":"879"},
            {"station_id":"113034","alt":"2850"},
            {"station_id":"110028","alt":"1801"},
            {"station_id":"105107","alt":"722"},#**
            {"station_id":"116027","alt":"3874"},#**
            {"station_id":"110008","alt":"497"},
            {"station_id":"116023","alt":"2130"},#**
            {"station_id":"112069","alt":"2470"},
            {"station_id":"114117","alt":"2964"}]

def get_df_table(station_id,alt,month):
    #station_id = "112056"
    #alt="3548"
    #month= "202206"
    dominio = "www.senamhi.gob.pe"
    subfolder= "mapas/mapa-estaciones-2"
    resource = "_dato_esta_tipo02.php"
    estado="REAL"
    cate_esta="CP"
    parameters = f"estaciones={station_id}&CBOFiltro={month}&t_e=M&estado={estado}&cod_old=&cate_esta={cate_esta}&alt={alt}"
    url = f"https://{dominio}/{subfolder}/{resource}?{parameters}"
    print(url)
    tables = pd.read_html(url) #see examples here: https://pbpython.com/pandas-html-table.html
    df_table = tables[1]
    df_table = df_table.iloc[1: , :]
    columns=[
        "date",
        "temperature_max",
        "temperature_min",
        "humidity",
        "precipitation"]
    df_table = df_table.iloc[1:].set_axis(columns,axis=1)#.map({'S/D': 0})
    for col in columns[1:]:
        df_table[col] = df_table[col].replace('S/D', 0).replace('T', 0.1).astype(float)
    df_table["station_id"] = station_id
    df_table = df_table[["station_id"] + columns]
    return df_table

def df_load_to_bq(df):
    #table_name = "estacion_convencional"
    table_schema=[
            {"name":"station_id","type":"STRING"},
            {"name":"date","type":"STRING"},
            {"name":"temperature_max","type":"FLOAT"},
            {"name":"temperature_min","type":"FLOAT"},
            {"name":"humidity","type":"FLOAT"},
            {"name":"precipitation","type":"FLOAT"},
        ]
    df.to_gbq(f"{DATASET}.{TABLE_NAME}",project_id=PROJECT_ID,
            if_exists="append",
            table_schema=table_schema)
# %%
list_df_table = []
for station in stations:
    print(station["station_id"])
    list_months = pd.date_range('2017-01-01','2022-06-01', freq='MS').strftime("%Y%m").tolist()
    for month in list_months:
        #print(month)
        df_table = get_df_table(**{"station_id" : station["station_id"],
                                    "alt" : station["alt"],
                                    "month" : month})
        list_df_table.append(df_table)
# %%
df_table_to_bq = pd.concat(list_df_table, axis=0).reset_index(drop=True)

 # %%
df_load_to_bq(df_table_to_bq)
"""
SELECT FORMAT_DATETIME("%Y%m", date(date)) m,count(1) FROM `proyecto-bigdata-318002.raw_zone.estacion_convencional` 
group by 1
order by 1 asc
"""
# %%
