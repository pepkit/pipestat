import yacman

from typing import Optional

from pydantic import create_model
from sqlmodel import Field, Session, SQLModel, create_engine, select

yam = yacman.YAMLConfigManager(filepath="tests/demo_pipestat_output_schema.yaml")

yam
yam.to_dict()


# Create the table column definitions matching this schema
sample_field_definitions = {
	"id": (Optional[int], Field(default=None, primary_key=True)),
}
for element in yam["properties"]["samples"]["items"]["properties"]:
	sample_field_definitions[element] = (Optional[str], Field(default=None))

project_field_definitions = {}
for element in yam["properties"]:
	if element == "samples":
		continue
	project_field_definitions[element] = (Optional[str], Field(default=None))

# Build a SQLModel for this JSON-schema
Sample = create_model(
	f"{yam['pipeline_id']}_sample", # Name of table
	__base__=SQLModel,
	__cls_kwargs__={"table": True},  # Specifies this for SQLModel table
	**sample_field_definitions
	)

Project = create_model(
	f"{yam['pipeline_id']}_project",
	**project_field_definitions
	)



sample_instance = {"smooth_bw":"path/to/smooth_bw.bw"}
project_instance = {"number_of_things": 15}


## Create an instance like this:
p1 = Project(**project_instance)

s1 = Sample(**sample_instance)
s1.smooth_bw

# or like this:
Sample(smooth_bw="path/to/smooth_bw.bw")

# Set attributes like this
p1.counts_table = "25"
p1

isinstance(s1, SQLModel)
isinstance(p1, SQLModel)


# Test database insertion
sqlite_url = "sqlite:///test.db"
engine = create_engine(sqlite_url, echo=True)
SQLModel.metadata.create_all(engine)
session = Session(engine)
s1 = Sample(**sample_instance)
s2 = Sample(**sample_instance)
type(s1)
session.add(s2)
session.commit()
session.refresh(s2)
print(s1)
print(s2)






from typing import Optional

from pydantic import create_model
from sqlmodel import Field, Session, SQLModel, create_engine


field_definitions = {
    "id": (Optional[int], Field(default=None, primary_key=True)),
    "name": (str, ...),
    "secret_name": (str, ...),
    "age": (Optional[int], None),
}

Hero = create_model(
    "Hero",
    __base__=SQLModel,
    __cls_kwargs__={"table": True},
    **field_definitions,
)

if __name__ == '__main__':
sqlite_url = "sqlite:///test.db"
engine = create_engine(sqlite_url, echo=True)
SQLModel.metadata.create_all(engine)
session = Session(engine)
hero = Hero(name="Spider-Boy", secret_name="Pedro Parqueador")
session.add(hero)
session.commit()
session.refresh(hero)
print(hero)

