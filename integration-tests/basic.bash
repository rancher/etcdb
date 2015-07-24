PREFIX="etcd-test-$$"
POSTGRES_IMAGE="yvess/alpine-postgres"


docker-ip() {
  local docker_addr="${DOCKER_HOST#tcp://}"
  echo "${docker_addr%:*}"
}


container-port() {
  local addr="$(docker port $1 $2)"
  echo "${addr#*:}"
}


setup() {
  docker build -t "$PREFIX-etcdb" .

  docker pull "$POSTGRES_IMAGE" > /dev/null
  docker run --name "$PREFIX-postgres" -P -d \
    -e PGDATA=/var/services/data/postgres \
    -e PGUSER=etcdb \
    -e PGPASSWORD=etcdb \
    -e PGDB=etcdb \
    "$POSTGRES_IMAGE" > /dev/null
  sleep 2

  local db_conn="user=etcdb password=etcdb dbname=etcdb host=db sslmode=disable"
  local public_ip=$(docker-ip)

  docker run --rm -it \
    --link "$PREFIX-postgres:db" \
    "$PREFIX-etcdb" \
    -init-db postgres "$db_conn"

  docker run --name "$PREFIX-etcdb" -d \
    --link "$PREFIX-postgres:db" \
    -p 23790:2379 \
    "$PREFIX-etcdb" \
    -advertise-client-urls http://$public_ip:23790 \
    -listen-client-urls http://0.0.0.0:2379 \
    postgres "$db_conn"

  sleep 2

  export ETCDCTL_PEERS="http://$public_ip:23790"
}
setup


teardown() {
  local containers="$(docker ps -q -f name=$PREFIX)"
  echo "$containers" | xargs docker rm -f > /dev/null
  docker rmi "$PREFIX-etcdb" > /dev/null
}
trap teardown EXIT


cleanKeys() {
  etcdctl ls / | xargs -n1 etcdctl rm --recursive
}


getField() {
  cat | grep $1 | fieldValue
}


fieldValue() {
  local line=$(head -n1)
  echo ${line#*: }
}


T_setGetDelete() {
  cleanKeys

  result="$(etcdctl ls /)"
  if [[ "$result" != "" ]]; then
    $T_fail "initial listing should be empty"
    return
  fi

  etcdctl set /foo bar > /dev/null

  result="$(etcdctl ls /)"
  if [[ "$result" != "/foo" ]]; then
    $T_fail "listing should contain /foo, but got $result"
    return
  fi

  result="$(etcdctl get /foo)"
  if [[ "$result" != "bar" ]]; then
    $T_fail "value of /foo should be bar, but got $result"
    return
  fi

  etcdctl rm /foo

  result="$(etcdctl ls /)"
  if [[ "$result" != "" ]]; then
    $T_fail "listing should be empty again after removing /foo"
    return
  fi
}

T_expire() {
  cleanKeys

  local createdIndex=$(etcdctl -o extended set --ttl 5 /foo bar | getField Created-Index)
  if [[ "$createdIndex" == "" ]]; then
    $T_fail "should have gotten the created index value"
    return
  fi

  result="$(etcdctl get /foo)"
  if [[ "$result" != "bar" ]]; then
    $T_fail "value should be set to bar, but got: $result"
    return
  fi

  local prevValue=$(etcdctl -o extended watch --after-index $createdIndex /foo | getField PrevNode.Value)

  if [[ "$prevValue" != "bar" ]]; then
    $T_fail "should get previous value of bar, but got: $prevValue"
  fi
}
