# OpenMeteo Load

## Getting Started
The required libraries can be installed into a virtual environment using:

```cmd
pip install -r requirements.txt
```

Running `main.py` will perform the data load from OpenMeteo, storage in Iceberg, and will plot a chart showing the mean temperatures across the 50 request cities.
```cmd
python main.py
```

The code is set to query 50 test locations, and a set of weather variables which I have chosen as they seem likely to affect supply of and demand for energy.

## Design decisions

- __Storage__ - I have decided to experiment with using Iceberg as a means to store Parquet files. It's something I have wanted to have a go at anyway, and its capabilities to store and access large amounts of data flexibly - allowing evolution over time - and supporting snapshotting made it seem like a suitable solution for this problem.

- __API Access__ - I've used the openmeteo-requests library to abstract some of the boilerplate API calls if using the requests library. A library like this can have downsides if poorly maintained or may lack flexibility if incomplete; however, this library seems reliably maintained and fairly complete.

- __Generic (Utility) Code__ - I've abstracted generic functionality for using Iceberg and OpenMeteo into the `utils` directory. These could be used in other projects and allow for project code to be decoupled from the libraries used to interact with these products.

### High-Level Overview
```mermaid
graph LR
    A[OpenMeteoAPI] --> B[OpenMeteo SDK]
    B --> C[Numpy Arrays]
    C --> D[PyArrow Table]
    D --> E[Iceberg]
```

## Productionising

- __Underlying Storage__ - For testing purposes, the data is being stored in a local directory with SQLite used for the metastore. A production system could use cloud blob storage or on-prem storage (such as Hive).

- __Environments__ - A production setup should have different, isolated environments (e.g. Local/QA/Prod) with config files.

- __Orchestration__ - Airflow could be used to run the data load on a schedule. Different endpoints could be loaded in separate tasks.

- __Containerising__ - It would be advisable to containerise the application so that it runs in a consistent manner.

- __Testing__ - Unit tests should be written.

## Potential Further Work

- __Concurrency__ - Add concurrency to the `process_response()` function to process locations more quickly.

- __Partitioning__ - Consider how the data is partitioned in Iceberg. Extract time, forecast time, and variable name are good candidates for partitioning.

- __Package Management__ - I would be worth switching to use poetry for package management if project were to become more complex.

- __Extend API Options__ - Extend the OpenMeteo code in `utils` to increase functionality


## Extra Information

Development Time: c.4-5 hours

AI Used:
- Chat GPT
    - Bouncing initial high-level ideas
    - Generating list of 50 test location latitude/longitudes
    - Generating Mermaid diagram in notes
    - Exploration of Iceberg and PyArrow implementation details
- GitHub Copilot
