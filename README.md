# big-data-geolocation
Big-Data Project

## Setup Docker
- git clone https://github.com/UUrrii/big-data-geolocation
- docker-compose up -d

## Setup Airflow
- docker exec -it airflow bash
- cd /home/airflow/scripts
- chmod +x setup.sh
- ./setup.sh

## Setupd Hadoop
- docker exec -it airflow bash
- cd
- su hadoop
- start-all.sh

## Prozesse
### Airflow
1. Lokalen Ordner erstellen
2. Inhalt des lokalen Ordners löschen
3. Geolocation Data downloaden
4. Geolocation Data unzippen (die 2 wichtigen Dateien: ipv4-ranges, geolocation-data-de)
5. HDFS directory für beide Dateien erstellen (parallel)
6. Beide Dateien in die HDFS directories verschieben
7. Dateien über pyspark verarbeiten und jeweils in eine MySQL-Datenbanktabelle abspeichern (Verarbeitung: nur die notwendigen Spalten behalten)

### Backend
- Frontend ist im Backend integriert unter dem Port 3000
- GET-Request, der zu einer IP-Adresse aus den Datenbanktabellen jeweils Kontinent, Land, Subdivison und Stadt liefert
