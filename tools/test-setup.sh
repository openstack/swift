#!/bin/bash -xe

# Set up a partition formatted with XFS to use as TMPDIR for our tests.
# OpenStack CI will invoke this script as part of tox based tests.
# The file .zuul.yaml set TMPDIR to $HOME/xfstmp.

# Create a large-ish file that we will mount as a loopback
truncate -s 1GB $HOME/1G_xfs_file
# Format the new file as XFS.
/sbin/mkfs.xfs $HOME/1G_xfs_file
# loopback mount the file
mkdir -p $HOME/xfstmp
sudo mount -o loop,noatime,nodiratime $HOME/1G_xfs_file $HOME/xfstmp
sudo chmod 777 $HOME/xfstmp

# Install liberasurecode-devel for CentOS from RDO repository.

function is_rhel8 {
    [ -f /usr/bin/dnf ] && \
        cat /etc/*release | grep -q -e "Red Hat" -e "CentOS" -e "CloudLinux" && \
        cat /etc/*release | grep -q 'release 8'
}
function is_rhel9 {
    [ -f /usr/bin/dnf ] && \
        cat /etc/*release | grep -q -e "Red Hat" -e "CentOS" -e "CloudLinux" && \
        cat /etc/*release | grep -q 'release 9'
}


if is_rhel8; then
    # Install CentOS OpenStack repos so that we have access to some extra
    # packages.
    sudo dnf install -y centos-release-openstack-xena
    sudo dnf install -y liberasurecode-devel
fi

if is_rhel9; then
    # Install CentOS OpenStack repos so that we have access to some extra
    # packages.
    sudo dnf install -y centos-release-openstack-yoga
    sudo dnf install -y liberasurecode-devel
fi
