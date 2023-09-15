For development, ensure you are running a docker instance hosting a postgressql DB whcih contains previously reported pipestat results:

in reader.py change path to pipestatconfig yaml which will have the DB information, e.g.:
`pipestatcfg = "/home/drc/PythonProjects/pipestat/testimport/drcdbconfig.yaml"`

navigate to pipestat reader folder in terminal:
` uvicorn reader:app --reload
`

