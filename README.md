# Take Home Test

This file contains info regarding assumption and implementations on take home task. Also it enlightens the process of running the task

## Assumptions

1. Dimensions are SCD2.
2. Primary key for invoices is invoice_id
3. Primary key for contracts is contracts_id

## Solution
##### Solution is made for batch processing but ELT would work the same if we were ingesting data with streams.

### Steps
1. Ingest Data in raw tables from json using python code
2. Then trigger the stored procedure that has the logic of transformation and loading into the final tables
3. Each task has its own function(task_1() etc) callable in main.py. This prints out the analysis that is required in each task

## Installation

#### For DB

```bash
docker build -t my-postgres-db ./
docker run -d --name my-postgresdb-container -p 5440:5432 my-postgres-db
```

#### Libraries (Python 3.8)

```bash
pip install psycopg2-binary
```

## Time Taken

1. Coding: 3 hrs
2. Logic Building : 5 hrs( because of rework  as my initial understanding of data was wrong)
3. Deployment etc: 1 hr

## Challenges

The biggest challenge i faced was the understanding of data. As the data and the test statement sometimes seemed were not aligning together.

## Notes

The container will use 5440 port on localhost.

