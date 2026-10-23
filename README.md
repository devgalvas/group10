# Maritime Traffic & Port Congestion Pipeline (AIS)

DAT535 Data Engineering, University of Stavanger, Group 10.

A batch Medallion pipeline in PySpark and Delta Lake. It ingests the daily AIS files published by
the Danish Maritime Authority, cleans and models them into a Silver layer of vessel positions and
vessel/port dimensions, and builds Gold tables of port calls and hourly congestion for Esbjerg,
Aarhus and Copenhagen. A Streamlit dashboard reads the Gold tables.

> **Status:** in development.

- Team guide and plan: [docs/TEAM_GUIDE.md](docs/TEAM_GUIDE.md)
- Archived course lab material: [labs/](labs/)
