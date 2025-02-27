#!/usr/bin/env bash
# tg_ubot/wait-for-it.sh — скрипт ожидания готовности TCP-сервиса.
# Используется для того, чтобы дождаться Kafka (или другого сервиса), прежде чем запускать бота.

WAITFORIT_cmdname=${0##*/}

echoerr() { echo "$@" 1>&2; }

usage()
{
  cat << USAGE >&2
Usage:
  $WAITFORIT_cmdname host:port [options] [-- command args]
Options:
  -s | --strict    Only execute subcommand if the test succeeds
  -q | --quiet     Don't output any status messages
  -t TIMEOUT       Timeout in seconds, zero for no timeout
  -- COMMAND ARGS   Execute command with args after the test finishes
USAGE
  exit 1
}

wait_for()
{
  if [[ $WAITFORIT_TIMEOUT -gt 0 ]]; then
    echoerr "$WAITFORIT_cmdname: waiting $WAITFORIT_TIMEOUT seconds for $WAITFORIT_HOST:$WAITFORIT_PORT"
  else
    echoerr "$WAITFORIT_cmdname: waiting for $WAITFORIT_HOST:$WAITFORIT_PORT without a timeout"
  fi
  start_ts=$(date +%s)
  while :
  do
    (echo > /dev/tcp/$WAITFORIT_HOST/$WAITFORIT_PORT) >/dev/null 2>&1 && break
    sleep 1
    current_ts=$(date +%s)
    if [[ $((current_ts - start_ts)) -ge $WAITFORIT_TIMEOUT ]]; then
      echo "timeout occurred after waiting $WAITFORIT_TIMEOUT seconds for $WAITFORIT_HOST:$WAITFORIT_PORT" >&2
      exit 1
    fi
  done
  echoerr "$WAITFORIT_cmdname: $WAITFORIT_HOST:$WAITFORIT_PORT is available"
}

# parse args
WAITFORIT_TIMEOUT=15
WAITFORIT_STRICT=0
WAITFORIT_HOST=
WAITFORIT_PORT=
WAITFORIT_CHILD=0

while [[ $# -gt 0 ]]
do
  case "$1" in
    *:* )
    WAITFORIT_HOST=$(echo $1 | cut -d : -f 1)
    WAITFORIT_PORT=$(echo $1 | cut -d : -f 2)
    shift 1
    ;;
    -t)
    WAITFORIT_TIMEOUT="$2"
    if [[ $WAITFORIT_TIMEOUT == "" ]]; then break; fi
    shift 2
    ;;
    --timeout=*)
    WAITFORIT_TIMEOUT="${1#*=}"
    shift 1
    ;;
    -s|--strict)
    WAITFORIT_STRICT=1
    shift 1
    ;;
    -q|--quiet)
    WAITFORIT_QUIET=1
    shift 1
    ;;
    --)
    shift
    WAITFORIT_CLI=("$@")
    break
    ;;
    *)
    usage
    ;;
  esac
done

if [ "$WAITFORIT_HOST" = "" -o "$WAITFORIT_PORT" = "" ]; then
  echo "Error: you need to provide a host and port to test."
  usage
fi

wait_for
WAITFORIT_RESULT=$?

if [[ $WAITFORIT_CLI != "" ]]; then
  if [[ $WAITFORIT_RESULT -ne 0 && $WAITFORIT_STRICT -eq 1 ]]; then
    echoerr "$WAITFORIT_cmdname: strict mode, refusing to execute subprocess"
    exit $WAITFORIT_RESULT
  fi
  exec "${WAITFORIT_CLI[@]}"
else
  exit $WAITFORIT_RESULT
fi
