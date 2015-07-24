etcdb is a work-in-progress to provide an implementation of the etcd REST API
on top of a SQL backend.

# Contact
For bugs, questions, comments, corrections, suggestions, etc., open an issue in
 [rancher/rancher](//github.com/rancher/rancher/issues) with a title starting with `[etcdb] `.

Or just [click here](//github.com/rancher/rancher/issues/new?title=%5Betcdb%5D%20) to create a new issue.

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
