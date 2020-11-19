# Database config specification

In order to use [PostgreSQL](https://www.postgresql.org/) database as the backend pipestat requires another input -- database configuration file.

This file consists of all the required data to connect and authenticate a pipestat user to a running PostgreSQL instance: 

```yaml
database:
  name: <database name>
  user: <user name>
  password: <user password>
  host: <database address>
  port: <database port>
```

## Example

If the PortgreSQL instance has been started in a container, with the following command:

```console
docker run -d 
    --name pipestat-postgres \ 
    -p 5432:5432 \ 
    -e POSTGRES_PASSWORD=b4fd34f^Fshdwede \
    -e POSTGRES_USER=john \ 
    -e POSTGRES_DB=pipestat-test \ 
    -v postgres-data:/var/lib/postgresql/data postgres
```

The configuration file should look like this:

```yaml
database:
  name: pipestat-test
  user: john
  password: b4fd34f^Fshdwede
  host: localhost
  port: 5432
```