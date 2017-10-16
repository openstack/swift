#!/usr/bin/env bash

# Client constraint file contains this client version pin that is in conflict
# with installing the client from source. We should remove the version pin in
# the constraints file before applying it for from-source installation.

set -e

if [[ -z "$CONSTRAINTS_FILE" ]]; then
    echo 'WARNING: expected $CONSTRAINTS_FILE to be set' >&2
    PIP_FLAGS=(-U)
else
    # NOTE(tonyb): Place this in the tox enviroment's log dir so it will get
    # published to logs.openstack.org for easy debugging.
    localfile="$VIRTUAL_ENV/log/upper-constraints.txt"

    if [[ "$CONSTRAINTS_FILE" != http* ]]; then
        CONSTRAINTS_FILE="file://$CONSTRAINTS_FILE"
    fi
    curl "$CONSTRAINTS_FILE" --insecure --progress-bar --output "$localfile"

    pip install -c"$localfile" openstack-requirements

    # This is the main purpose of the script: Allow local installation of
    # the current repo. It is listed in constraints file and thus any
    # install will be constrained and we need to unconstrain it.
    edit-constraints "$localfile" -- "$CLIENT_NAME"

    # Also unconstrain eventlet, as our s3tests results rely on a more-recent
    # version
    edit-constraints "$localfile" -- eventlet
    # Swift also hard-depends on a more-recent pyeclib
    edit-constraints "$localfile" -- pyeclib

    PIP_FLAGS=(-c"$localfile" -U)
fi

pip install "${PIP_FLAGS[@]}" "$@"
