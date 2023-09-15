For development, ensure you are running a docker instance hosting a postgressql DB whcih contains previously reported pipestat results:

Docker command:
```
sudo docker run --rm -it -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=pipestat-password -e POSTGRES_DB=pipestat-test -p 5432:5432 postgres
```

in reader.py change path to pipestatconfig yaml which will have the DB information, e.g.:
`pipestatcfg = "/home/drc/PythonProjects/pipestat/testimport/drcdbconfig.yaml"`

navigate to pipestat reader folder in terminal:
` uvicorn reader:app --reload
`

