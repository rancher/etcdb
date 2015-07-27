etcdb is a work-in-progress to provide an implementation of the etcd REST API
on top of a SQL backend.

# Contact
For bugs, questions, comments, corrections, suggestions, etc., open an issue in
 [rancher/rancher](//github.com/rancher/rancher/issues) with a title starting with `[etcdb] `.

Or just [click here](//github.com/rancher/rancher/issues/new?title=%5Betcdb%5D%20) to create a new issue.

# Usage

## Database setup

To create the required database tables, run the `etcdb` command once with the
`-init-db` flag. This will create the tables and then exit (see below for
the database connection parameters):

```
etcdb -init-db <database type> <connection parameters>
```

## Starting the server

Etcdb supports either MySQL or Postgres backend databases. The `etcdb` command
takes two required arguments, the type of database to connect to, and
parameters for the database connection. Here are examples with the most commonly
specified parameters:

```
etcdb postgres "user=username password=password host=hostname dbname=dbname sslmode=disable"

etcdb mysql username:password@tcp(hostname:3306)/dbname
```

Additional parameters are documented for each of the Go database drivers:

* [Postgres connection parameters](https://godoc.org/github.com/lib/pq#hdr-Connection_String_Parameters)
* [MySQL connection parameters](https://github.com/go-sql-driver/mysql#dsn-data-source-name)

## Client connections

For compatibility with `etcd`, the `etcdb` server by default listens on ports
`2379` and `4001` on `localhost`.

To listen on other ports or network interfaces, `etcdb` takes two options
`-listen-client-urls` and `-advertise-client-urls` which are compatible with
the same options on the `etcd` server.

`-listen-client-urls` specifies the hosts and ports that the server will listen
for connections on.

`-advertise-client-urls` specifies the matching URLs that are accessible to
the client. When cluster aware clients such as `etcdctl` connect to the server,
this is the list of URLs it will "advertise" for these clients to connect to.

To listen on a public network interface, these options can have the same value:

```
etcdb \
  -listen-client-urls http://10.0.0.1:92379 \
  -advertise-client-urls http://10.0.0.1:92379 \
  postgres "sslmode=disable"
```

However, if for example, you're running the server in a Docker container,
forwarding the external port `92379` to the container's port `2379`. You would
would start `etcdb` listening for connections on all container IPs on port
`2379`, but *advertise* the client URL with the publicly accessible IP and port
number:

```
etcdb \
  -listen-client-urls http://0.0.0.0:2379 \
  -advertise-client-urls http://${PUBLIC_IP}:92379 \
  postgres "sslmode=disable"
```

# Testing

## Unit tests

The Makefile has a helper for running the Go tests:

```
make test
```

## Integration testing

The `integration-tests` directory contains tests using the `etcdctl` command to
exercise the public etcd-compatible API.

The integration tests are written in Bash and use
[basht](https://github.com/progrium/basht) to run. This can be installed with:

```
make test-deps
```

The tests also expect `etcdctl` and `docker` to be on the path. The
`DOCKER_HOST` environment variable needs to be set to a `tcp://` address since
the tests need the Docker IP for `etcdctl` to connect to.

Run the integration tests with:

```
make test-integration
```
