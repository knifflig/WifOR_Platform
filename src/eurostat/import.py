# for flags (symbols and abbreviations) in Eurostat see https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Tutorial:Symbols_and_abbreviations#Statistical_symbols.2C_abbreviations_and_units_of_measurement

import eurostat
import pandas as pd

toc = eurostat.get_toc()
toc[0]
toc[12:15]

# Liste aller verfügbaren Datensätze
toc_df = eurostat.get_toc_df()
toc_df

# Liste durchsuchen nach Keyword
f = eurostat.subset_toc_df(toc_df, "employment")

# Import Dataset as pandas dataframe zum Thema employment (beliebig erweiterbar)
###### WICHTIG: Beim Hinzufügen neuer Links, diese immer hinten einfügen!!!! ####
data1 = eurostat.get_data_df("lfsa_egan2") # Zeit, Land, Geschlecht, Alter, NACE 2
data2 = eurostat.get_data_df("lfsa_egan") # Zeit, Land, Geschlecht, Nationalität, Alter
data3 = eurostat.get_data_df("lfsa_eisn2") # Zeit, Land, ISCO1, NACE2, Geschlecht, Alter
data4 = eurostat.get_data_df("lfsa_egai2d") # Zeit, Land, ISCO2, Geschlecht
data5 = eurostat.get_data_df("lfsa_ugad") # Zeit, Land, Geschlecht, Dauer Alo, Alter
data6 = eurostat.get_data_df("lfsa_ugpis") # Zeit, Land, Geschlecht, ISCO1 Alo
data7 = eurostat.get_data_df("lfst_r_lfe2en2") # Region NUTS2, Zeit, NACE2, Alter, Geschlecht
data8 = eurostat.get_data_df("lfsa_egaisedm") # Beschäftigung nach Geschlecht, Alter, Migrationsstatus, Beruf und Bildungsabschluss

#data_ETQ = eurostat.get_data_df("lfsa_argan")

# DataFrames data1 bis data7 filtern und Bereinigungen vornehmen
datasets = [data1, data2, data3, data4, data5, data6, data7, data8]

# Pfad zum Speichern der CSV-Dateien (ggf. ändern)
export_path = r'D:\WifOR\Arbeitsmarkt - Dokumente\05 Projekte\2023\FKM OOE Neuentwicklung\2_Berechnungen\1_Rohdaten\Eurostat'

# Schleife zum Exportieren der DataFrames
for i, dataset in enumerate(datasets, 1):
    # Dateiname für die CSV-Datei
    filename = f"{export_path}\\data{i}.csv"

    # CSV exportieren mit Tabulator als Trennzeichen und ohne Index
    dataset.to_csv(filename, encoding="latin1", sep=";", decimal=",", index=False)

# DataFrames data1 bis data7 filtern und Bereinigungen vornehmen
datasets = [data1, data2, data3, data4, data5, data6, data7, data8]

# Datensätze nach Ländern und Referenzzeitraum filtern und entsprechend kürzen
selected_countries = ['DE', 'AT', 'EE'] #Länderauswahl
reference_year = "2022" #Spalten die kleiner als Ist-Jahr sind werden entfernt

# DataFrames data1 bis data6 filtern und Bereinigungen vornehmen und benennen
datasets = [data1, data2, data3, data4, data5, data6, data7, data8]
df = {}

for i, dataset in enumerate(datasets):
    # Filtern nach ausgewählten Ländern
    dataset = dataset[dataset['geo\TIME_PERIOD'].isin(selected_countries)]

    # Entfernen von Spalten mit Spaltenbezeichnungen kleiner als Ist-Jahr
    columns_to_remove = [col for col in dataset.columns if col.isnumeric() and int(col) < int(reference_year)]
    dataset = dataset.drop(columns=columns_to_remove, axis=1)

    # Benennen und im Dictionary speichern (df1 bis ...)
    df[f"df{i + 1}"] = dataset

