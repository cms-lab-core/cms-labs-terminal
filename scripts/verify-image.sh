#!/bin/sh
set -eu

IMAGE="${1:-cms-labs-terminal:dev}"
failures=0

check() {
    description="$1"
    shift
    if "$@" >/dev/null 2>&1; then
        printf 'ok    %s\n' "${description}"
    else
        printf 'FAIL  %s\n' "${description}"
        failures=$((failures + 1))
    fi
}

refute() {
    description="$1"
    shift
    if "$@" >/dev/null 2>&1; then
        printf 'FAIL  %s\n' "${description}"
        failures=$((failures + 1))
    else
        printf 'ok    %s\n' "${description}"
    fi
}

check 'the broker binary runs' docker run --rm --entrypoint /cms-labs-terminal "${IMAGE}" version
check 'ttyd is present and executable' docker run --rm --entrypoint /usr/bin/ttyd "${IMAGE}" --version
refute 'no shell is installed' docker run --rm --entrypoint /bin/sh "${IMAGE}" -c true
refute 'no package manager is installed' docker run --rm --entrypoint /sbin/apk "${IMAGE}" --version
refute 'no device-side session multiplexer is bundled' docker run --rm --entrypoint /usr/bin/tmux "${IMAGE}" -V
refute 'no dynamic loader is installed' docker run --rm --entrypoint /lib64/ld-linux-x86-64.so.2 "${IMAGE}" --version

printf '\nimage: %s\n' "$(docker image inspect --format '{{.Id}}' "${IMAGE}")"
printf 'size:  %s bytes\n' "$(docker image inspect --format '{{.Size}}' "${IMAGE}")"

if [ "${failures}" -ne 0 ]; then
    printf '\n%s check(s) failed\n' "${failures}" >&2
    exit 1
fi

printf '\nall checks passed\n'
