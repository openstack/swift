#!/bin/bash
set -euxo pipefail

# install dependencies
apt-get update
apt-get dist-upgrade -y
DEPENDS="netbase ca-certificates curl sudo rsync gettext liberasurecode1 libffi7 libssl1.1 netcat procps lsof iproute2"
MAKEDEPENDS="git build-essential liberasurecode-dev libffi-dev libssl-dev"
apt-get install -y --no-install-recommends ${DEPENDS} ${MAKEDEPENDS}

# create service user
groupadd -g 1000 swift
useradd -u 1000 -g swift -M -d /var/lib/swift -s /usr/sbin/nologin -c "swift user" swift
install -d -m 0755 -o swift -g swift /etc/swift /var/log/swift /var/lib/swift /var/cache/swift

RELEASE="2024.1"
# fetch upper-constraints.txt from openstack/requirements
if [ "${BUILD_MODE}" = sap ]; then
  # Atm there are only upper-constraints from the previous release:
  #   https://github.com/openstack/requirements/compare/stable/zed...sapcc:requirements:stable/zed-m3
  # The only possible relevant change is in keystonemiddleware:
  # - https://github.com/openstack/keystonemiddleware/compare/stable/zed...sapcc:keystonemiddleware:stable/zed-m3
  # - The severity of a log message was decreased from ERROR to INFO because of Sentry
  # - Swift does not use Sentry
  # Because there are no relevant sapcc upper-constraints for Swift, we choose the upstream upper-constraints
  #curl -L -o /root/upper-constraints.txt https://raw.githubusercontent.com/sapcc/requirements/stable/${RELEASE}-m3/upper-constraints.txt

  curl -L -o /root/upper-constraints.txt https://raw.githubusercontent.com/openstack/requirements/stable/${RELEASE}/upper-constraints.txt

# Current decision, we do not update libraries without an upstream fix
#  #########################
#  # vulnerability patches #
#  #########################
# PIP
# https://avd.aquasec.com/nvd/cve-2023-5752
  pip install --upgrade pip
#
#  jinja2 security patch update
#  CVE-2024-22195
  sed -i '/Jinja2===/c\Jinja2===3.1.4' /root/upper-constraints.txt
#  # Check for vulnerability in https://dashboard.eu-de-1.cloud.sap/ccadmin/master/keppel/#/repo/ccloud/swift and tag ${RELEASE}-latest
#  # and update upper-constraints accordingly
#  #
#  # https://avd.aquasec.com/nvd/cve-2023-0286
#  # https://github.com/advisories/GHSA-39hc-v87j-747x
#  # https://avd.aquasec.com/nvd/cve-2023-23931
#  # https://github.com/advisories/GHSA-5cpq-8wj7-hf2v
#  https://avd.aquasec.com/nvd/cve-2024-26130
# pyOpenSSL version need to change to support latest cryptography
  sed -i '/pyOpenSSL===/c\pyOpenSSL===24.1.0' /root/upper-constraints.txt
  sed -i '/cryptography===/c\cryptography===42.0.4' /root/upper-constraints.txt
#
#  # pyopenssl 22.1.0 depends on cryptography<39 --> update pyopenssl
#  sed -i '/pyOpenSSL===/c\pyOpenSSL===23.2.0' /root/upper-constraints.txt
#
#  # https://avd.aquasec.com/nvd/cve-2023-32681
#  sed -i '/requests===/c\requests===2.31.0' /root/upper-constraints.txt
#
#  # https://avd.aquasec.com/nvd/cve-2023-28859
#  sed -i '/redis===/c\redis===4.4.4' /root/upper-constraints.txt
#
# urllib3 patch
# https://avd.aquasec.com/nvd/cve-2023-43804
  sed -i '/urllib3===/c\urllib3===1.26.19' /root/upper-constraints.txt
#
#  requests security patch update
#  CVE-2023-32681
  sed -i '/requests===/c\requests===2.32.0' /root/upper-constraints.txt

#
# setuptools patch
# CVE-2024-6345
  rm -rf /usr/local/lib/python3.11/site-packages/setuptools*

#
# idna patch
# CVE-2024-3651
  sed -i '/idna===/c\idna===3.7' /root/upper-constraints.txt

#
# dnspython patch
# CVE-2023-29483
  sed -i '/dnspython===/c\dnspython===2.6.1' /root/upper-constraints.txt

#
# eventlet patch
# CVE-2023-29483
  sed -i '/eventlet===/c\eventlet===0.35.2' /root/upper-constraints.txt


#  #############################
#  # end vulnerability patches #
#  #############################
else
  curl -L -o /root/upper-constraints.txt https://raw.githubusercontent.com/openstack/requirements/stable/${RELEASE}/upper-constraints.txt
fi

# fetching origin to get an up to date tag list
# needed for pbr and a proper swift version string
git -C /opt/swift fetch origin

# setup virtualenv and install Swift there
python3.11 -m venv /opt/venv/
set +ux; source /opt/venv/bin/activate; set -ux
pip_install() {
  pip --no-cache-dir install --upgrade "$@"
}
pip_install pip
pip_install setuptools wheel
pip_install --no-compile -c /root/upper-constraints.txt \
  /opt/swift/ \
  keystonemiddleware \
  python-memcached \
  python-keystoneclient \
  python-swiftclient \
  pyOpenSSL

# if requested, install components required by the Helm chart at
# https://github.com/sapcc/helm-charts/tree/master/openstack/swift
if [ "${BUILD_MODE}" = sap ]; then
  curl -L -o /usr/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_x86_64
  chmod +x /usr/bin/dumb-init

  pip_install --no-compile -c /root/upper-constraints.txt \
    git+https://github.com/sapcc/swift-account-caretaker.git \
    git+https://github.com/sapcc/swift-addons.git \
    git+https://github.com/sapcc/openstack-watcher-middleware.git@1.0.34 \
    git+https://github.com/sapcc/openstack-rate-limit-middleware.git@1.1.0 \
    git+https://github.com/sapcc/openstack-audit-middleware.git@swift_audit_map

  # apply keystonemiddleware patch
  (
    cd /opt/venv/lib/python3.11/site-packages && patch -p0
  ) < /opt/swift/docker-sap/keystonemiddleware-no-service-catalog-header.patch

  # startup logic and unmount helper
  install -D -m 0755 -t /usr/bin/ /opt/swift/docker-sap/bin/*
fi

# cleanup
apt-get purge -y --auto-remove ${MAKEDEPENDS}
rm -rf /var/lib/apt/lists/*

rm -rf /tmp/* /root/.cache
find /usr/ /var/ -type f -name '*.pyc' -delete
