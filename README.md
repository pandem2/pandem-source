# PANDEM - Source
Manage heterogeneous data sources for pandem 2 project. 

## Description
Identify, map and integrate multiple pandemic related data into a coherent pandemic-management database. Developed within the H2020 project PANDEM-2, Pandem-Source allows users to systematically capture, standardize and analyze data coming from international and national surveillance databases, participatory surveillance projects, social networks and mass media. This tool is focused on flexibility so adding new sources or variables can be easily done as is required during a pandemic episode. 

## Approach
Data integration follow a semantic approach. Data sources just need to be described using a Data Labelling Schema (DLS) file that specifies the acquisition chennel (URL, git, local file or script) the format and how to map the input data to Pandem-Source variables.

The DLS ensures each source and variable is properly documented knwing its origin, meaning and data quality.

## Supported sources
Pandem-Source is designed to be flexible and extensible so new sources can be easily added by end users. In order to demontstrate this principle. The following sources are supported Out of the Box

- [COVID19 Data Hub](https://covid19datahub.io/) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/covid19-datahub.json)
- [ICD-10-CM](https://www.cdc.gov/nchs/icd/icd10cm.htm) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/ICD-10-diseases-list.json) 
- [ECDC Atlas](https://www.ecdc.europa.eu/en/surveillance-atlas-infectious-diseases) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/ecdc-atlas-influenza.json)
- [ECDC COVID19 Datasets](https://www.ecdc.europa.eu/en/covid-19/data) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/ecdc-covid19-variants.json)
- [Influenza Net](http://www.influenzanet.info) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/influenza-net.json)
- [MediSys](https://medisys.newsbrief.eu/medisys/) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/medisys.json)
- [Eurostats NUTS](https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/administrative-units-statistical-units/nuts) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/nuts-eurostat.json)
- [Twitter](https://twitter.com) see [source definition](https://github.com/pandem2/pandem-source/blob/main/pandemsource/data/DLS/twitter.json)

## User Interface - timeseries explorer
![Time series](https://github.com/pandem2/pandem-source/raw/main/img/P2.timeseries.png)

## User Interface - Integration dashboard
![Integration dashboard](https://github.com/pandem2/pandem-source/raw/main/img/P2.Integration.png)

## Requirements
- Python 3.7 or higher
- R 3.6.3 or higher
- Docker (optional for supporting machine learning article classification)

## Installation
pip install pandem2-source

## Running Pandem-Source

- Set the PANDEM\_HOME variable to a local foder
``` 
export PANDEM\_HOME=your data folder here 
```
- Load defailt sources
```
python -m pandem2source reset --restore-factory-defaults
```
- Running monitoring and dashboard
```
python -m pandemsource start -d
```
- Accessing the dashboard from http://localhost:8001 to see the progress and integrated time series

# PANDEM 2

PANDEM-2 is a H2020 EU-funded project that aims to develop new solutions for efficient, EU-wide pandemic management. The goal of PANDEM-2 is to prepare Europe for future pandemics through innovations in training and to build capacity between EU member states responding to pandemics on a cross-border basis.


