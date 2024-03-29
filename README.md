# PANDEM - Source
Integrate heterogeneous publuc health surveillance data sources into a coherent data base for PANDEM-2 project. 

## Description
Identify, map and integrate multiple pandemic related data into a coherent pandemic-management database. Developed within the H2020 project PANDEM-2, Pandem-Source allows users to systematically capture, standardize and analyze data coming from international and national surveillance databases, participatory surveillance projects, social networks and mass media. This tool is focused on flexibility so adding new sources or variables can be easily done as is required during a pandemic episode. 

## Target users
Public health data experts and data managers needing integrate several surveillance data.

## Approach
Data integration follow a semantic approach. Data sources need to be described using a Data Labelling Schema (DLS) file that specifies the acquisition chennel (URL, git, local file or script) the format and how to map the input data to Pandem-Source variables. If further customistions are needed, the user can define them using simple python scripts.

The DLS ensures each source and variable is properly documented knwing its origin, meaning and data quality.

## Supported sources
Pandem-Source is designed to be flexible and extensible so new sources can be easily added by end users. In order to demontstrate this principle. The following sources are supported Out of the Box

### Sources for indicators
- [COVID19 Data Hub](https://covid19datahub.io/) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/covid19-datahub.json)
- [ECDC Atlas](https://www.ecdc.europa.eu/en/surveillance-atlas-infectious-diseases) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/ecdc-atlas-influenza.json)
- [ECDC COVID19 Datasets](https://www.ecdc.europa.eu/en/covid-19/data) see source definitions for [variants](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/ecdc-covid19-variants.json), [age group](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/ecdc-covid19-age-group.json), [daily cases](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/ecdc-covid19-daily.json), [goverments measures](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/ecdc-covid19-measures.json), [vaccination](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/ecdc-covid19-vaccination.json)
- [ECDC COVID19 Simulated data](https://github.com/maous1/Pandem2simulator) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/ecdc-covid19-age-group-variants.json)
- [Serotracker](https://serotracker.com/en/Explore) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/serotracker.json)
- [Open Sky Nerwork](https://opensky-network.org/) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/opensky-network-coviddataset.json)
- [Influenza Net](http://www.influenzanet.info) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/influenza-net.json)
- [MediSys](https://medisys.newsbrief.eu/medisys/) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/medisys.json)
- [Twitter](https://twitter.com) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/twitter.json)
- [User provided data](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/input-local-defaults/M.12%20Upload%20templates_end_users.xlsx?raw=true). You can use this data template to easily integrate your own real or ficticious datasets.

### Sources for referentials
- [Eurostats NUTS](https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/administrative-units-statistical-units/nuts) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/nuts-eurostat.json)
- [Our airports](https://ourairports.com/) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/ourairports.json)
- [Geonames](https://www.geonames.org/) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/geonames-countries.json)
- [ICD-10-CM](https://www.cdc.gov/nchs/icd/icd10cm.htm) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/ICD-10-siseases-list.json) 

## User Interface - timeseries explorer
![Time series](https://github.com/pandem2/pandem-source/raw/main/img/P2.timeseries.png)

## User Interface - Integration dashboard
![Integration dashboard](https://github.com/pandem2/pandem-source/raw/main/img/P2.Integration.png)

## Requirements
- Python 3.7 or higher
- R 3.6.3 or higher
- Docker (optional for supporting machine learning article classification)

## Installing from pip
If you want to customize the installation folder (defaulted to ~/.pandemsource) you need to set the environment variable PANDEM\_HOME to a different folder
```
pip install pandem-source

python -m pandemsource setup --install

```

# Installing for contributors
If you want to customize the installation folder (defaulted to ~/.pandemsource) you need to set the environment variable PANDEM\_HOME to a different folder

```
git clone https://github.com/pandem2/pandem-source

cd pandem-source

make init

make install

source env/bin/activate

python -m pandemsource setup --install

```

# Defining sources o monitor
PANDEM-Source comes with a list of predefined sources it can monitor, but they are not all enabled by default.
In order to use them them you have to manually activate them with the 'setup' command.
## See all avaiable sources

```
python -m pandemsource setup -h
```

## Activating a particular source
```
# Activating ECDC covid19 dataset monitoring
python -m pandemsource setup --ecdc-covid19

# Activating preloaded 2023 functional excercise data
python -m pandemsource setup --pandem-2-2023-fx
```
# Running Pandem-Source

- Set the PANDEM\_HOME variable to a local foder (only if you have customized the default installation folder)
``` 
export PANDEM\_HOME=your data folder here 
```
- Running monitoring and dashboard 
```
python -m pandemsource start -d 

```
- Accessing the dashboard from http://localhost:8001 to see the progress and integrated time series

## Troubleshooting

If the command `python -m pandemsource start -d --no-nlp` does not work. Please make sure you've installed *R 3.6.3 or higher*. If R is properly installed, please make sure you've also installed required dependencies with:

(In your terminal)
```bash
sudo apt install libxml2-dev libsodium-dev libssl-dev libcurl4-openssl-dev libgdal-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev 
```

(In the R interpreter)
```R
install.packages(c("epitweetr", "dplyr", "shiny", "plotly", "DT", "jsonlite", "httr", "XML", "ggplot2", "epitweetr", "reticulate", "seqinr", "readr"))
```


# PANDEM 2

PANDEM-2 is a H2020 EU-funded project that aims to develop new solutions for efficient, EU-wide pandemic management. The goal of PANDEM-2 is to prepare Europe for future pandemics through innovations in training and to build capacity between EU member states responding to pandemics on a cross-border basis.


