# Data Source Connections
Details of all the connection configuration supported can be found in the below subsections for each type of connection.

All connection details follow the same pattern.
```
<connection format> {
    <connection name> {
        <key> = <value>
    }
}
```

When defining a configuration value that can be defined by a system property or environment variable at runtime, you can define that via the following:
```
url = "localhost"
url = ${?POSTGRES_URL}
```
The above defines that if there is a system property or environment variable named `POSTGRES_URL`, then that value will be used for the `url`, otherwise,
it will default to `localhost`.


## File System

## JDBC
Follows the same configuration used by Spark as found [here](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html).  
Sample can be found below
```
jdbc {
    postgres {
        url = "jdbc:postgresql://localhost:5432/customer"
        url = ${?POSTGRES_URL}
        user = "postgres"
        user = ${?POSTGRES_USERNAME}
        password = "postgres"
        password = ${?POSTGRES_PASSWORD}
        driver = "org.postgresql.Driver"
    }
}
```

## Cassandra
Follows same configuration as defined by the Spark Cassandra Connector as found [here](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md)  

```
org.apache.spark.sql.cassandra {
    cassandra {
        spark.cassandra.connection.host = "localhost"
        spark.cassandra.connection.host = ${?CASSANDRA_HOST}
        spark.cassandra.connection.port = "9042"
        spark.cassandra.connection.port = ${?CASSANDRA_PORT}
        spark.cassandra.auth.username = "cassandra"
        spark.cassandra.auth.username = ${?CASSANDRA_USERNAME}
        spark.cassandra.auth.password = "cassandra"
        spark.cassandra.auth.password = ${?CASSANDRA_PASSWORD}
    }
}
```

## JMS
Uses JNDI lookup to send messages to JMS queue.
```
jms {
    solace {
        initialContextFactory = "com.solacesystems.jndi.SolJNDIInitialContextFactory"
        url = "smf://localhost:55555"
        vpnName = "default"
        user = "admin"
        password = "admin"  
    }
}
```
## HTTP
```
http {
    customer_api {
        url = ""
        user = "admin"      #optional
        password = "admin"  #optional
    }
}
```