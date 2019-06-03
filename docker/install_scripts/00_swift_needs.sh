#!/bin/sh
set -e

# adduser -D -H syslog && \
for user in "swift"; do
  if ! id -u $user > /dev/null 2>&1 ; then
    adduser -D $user
    printf "created user $user\n"
  fi
done
printf "\n"
# mkdir /srv/node && \
# mkdir /var/spool/rsyslog && \
# chown -R swift:swift /srv/node/ && \
for dirname in "/srv/node" "$HOME/bin" "/opt" "/var/cache/swift" " /var/log/socklog/swift" "/var/log/swift/" "/var/run/swift"; do
  if [ ! -d $dirname ]; then
    mkdir -p $dirname
    printf "created $dirname\n"
  fi
done
# mkdir -p $HOME/bin && \
# mkdir -p /opt
