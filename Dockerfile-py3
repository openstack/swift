################################################
#
#  Alpine 3.16.2 Swift-All-In-One
#
################################################

FROM        alpine:3.16.2
MAINTAINER  Openstack Swift

ENV	        S6_LOGGING 1
ENV	        S6_VERSION 1.21.4.0
ENV         SOCKLOG_VERSION 3.0.1-1
ENV	        ARCH amd64
ENV         BUILD_DIR "/tmp"
ENV         ENV="/etc/profile"

#COPY        docker/install_scripts /install_scripts
COPY        . /opt/swift

ADD	        https://github.com/just-containers/s6-overlay/releases/download/v$S6_VERSION/s6-overlay-$ARCH.tar.gz /tmp/
ADD	        https://github.com/just-containers/s6-overlay/releases/download/v$S6_VERSION/s6-overlay-$ARCH.tar.gz.sig /tmp/
ADD         https://github.com/just-containers/socklog-overlay/releases/download/v$SOCKLOG_VERSION/socklog-overlay-$ARCH.tar.gz /tmp/

RUN         mkdir /etc/swift && \
            echo && \
            echo && \
            echo && \
            echo "================   starting swift_needs  ===================" && \
            /opt/swift/docker/install_scripts/00_swift_needs.sh && \
            echo && \
            echo && \
            echo && \
            echo "================   starting apk_install_prereqs  ===================" && \
            /opt/swift/docker/install_scripts/10_apk_install_prereqs.sh && \
            echo && \
            echo && \
            echo && \
            echo "================   starting apk_install_py3  ===================" && \
            /opt/swift/docker/install_scripts/21_apk_install_py3.sh && \
            echo && \
            echo && \
            echo && \
            echo "================   starting swift_install  ===================" && \
            /opt/swift/docker/install_scripts/50_swift_install.sh && \
            echo && \
            echo && \
            echo && \
            echo "================   installing s6-overlay  ===================" && \
            gpg --import /opt/swift/docker/s6-gpg-pub-key && \
            gpg --verify /tmp/s6-overlay-$ARCH.tar.gz.sig /tmp/s6-overlay-$ARCH.tar.gz && \
            gunzip -c /tmp/s6-overlay-$ARCH.tar.gz | tar -xf - -C / && \
            gunzip -c /tmp/socklog-overlay-amd64.tar.gz | tar -xf - -C / && \
            rm -rf /tmp/s6-overlay*  && \
            rm -rf /tmp/socklog-overlay* && \
            echo && \
            echo && \
            echo && \
            echo "================   starting pip_uninstall_dev  ===================" && \
            /opt/swift/docker/install_scripts/60_pip_uninstall_dev.sh && \
            echo && \
            echo && \
            echo && \
            echo "================   starting apk_uninstall_dev  ===================" && \
            /opt/swift/docker/install_scripts/99_apk_uninstall_dev.sh && \
            echo && \
            echo && \
            echo && \
            echo "================ clean up  ===================" && \
            echo "TODO: cleanup"
            #rm -rf /opt/swift


# Add Swift required configuration files
COPY         docker/rootfs /

ENTRYPOINT	["/init"]
