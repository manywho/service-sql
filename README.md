ManyWho Sql Service
===================

This service allows you to read and write from a database.
Supported databases MySQL 5.1, PostgreSQL 8.4. and Sql Server 2008

There is a limit of 1000 rows for query, but it is possible to paginate using filters.
There is more documentation at the [wiki](https://github.com/manywho/service-sql/wiki). 

[![Build Status](https://travis-ci.org/manywho/service-sql.svg?branch=develop)](https://travis-ci.org/manywho/service-sql)

## Usage

If you need to run your own instance of the service (e.g. for compliance reasons), it's easy to spin up following these
instructions:

#### Building

To build the service, you will need to have Maven 3 and a Java 8 implementation installed.
You can build the runnable shaded JAR:

```bash
$ mvn clean package -DskipTests=true
```

#### Testing

Copy the file src\test\resources\configuration.properties.dist and paste it with name src\test\resources\configuration.properties
Your configuration of the database for test is the one that you have in configuration.properties

Example of running all the suites with all the databases (you should have all the databases configured and running before execute this command)

````
mvn clean test 
````

Or you can execute the test for a specific database

````
mvn clean test -Dtest=MySqlTestSuite
mvn clean test -Dtest=PostgreSqlTestSuite
mvn clean test -Dtest=SqlServerTestSuite
````

To set up any of the supported database in your local environment you can create them using docker as shows in .travis.yml


#### Running

The service is a Jersey JAX-RS application, that by default is run under the Grizzly2 server on port 8080 (if you use 
the packaged JAR).

##### Defaults

Running the following command will start the service listening on `0.0.0.0:8080/api/sql/2`:

```bash
$ java -jar target/sql-2.0-SNAPSHOT.jar
```

##### Custom Port

You can specify a custom port to run the service on by passing the `server.port` property when running the JAR. The
following command will start the service listening on port 9090 (`0.0.0.0:9090/api/sql/2`):

```bash
$ java -Dserver.port=9090 -jar target/sql-2.0-SNAPSHOT.jar
```

## Contributing

Contribution are welcome to the project - whether they are feature requests, improvements or bug fixes! Refer to 
[CONTRIBUTING.md](CONTRIBUTING.md) for our contribution requirements.

## License

This service is released under the [MIT License](http://opensource.org/licenses/mit-license.php).
