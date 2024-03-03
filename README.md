## The Task
An external system / supplier is sending patient data to our platform using the FHIR standard. Our analytics teams find this format difficult to work with when creating dashboards and visualizations. You are required to tranform these FHIR messages into a more workable format preferably in a tabular format. Include any documentation / commentary you deem necessary.


## The Solution
The process takes a number of json fhir files. Process them into a normalised dataframe and attaches them to a list of dictionaries based on the resource type. Each dataframe is inserted into a Postgres database for ease of access. This will enable an analytics team to search for the relevant data easier than parsing through a nested json file.

## Run Command
With docker installed on your machine, run the following command to set up the container and postgres environment.
```
docker run --name fhir_db -e POSTGRES_PASSWORD=my_password -d -p 5432:5432 postgres:13
```

Now to run the script, please run the following in the terminal.
```
python process_fhir_json.py
```

To run the unittest, please run the following into the terminal.
```
python process_fhir_json_test.py
```

To stop the docker process, run this command.
```
docker stop fhir_db
```