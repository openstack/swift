# SAIO (Swift All in One)

SAIO is a containerized instance of Openstack Swift object storage. It is
running the main services of Swift, designed to provide an endpoint for
application developers to test against both the Swift and AWS S3 API. It can
also be used when integrating with a CI/CD system. These images are not
configured to provide data durability and are not intended for production use.


# Quickstart

```
docker pull openstackswift/saio
docker run -d -p 8080:8080 openstackswift/saio
```

### Test against Swift API:

Example using swift client to target endpoint:
```
swift -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing stat
```

### Test against S3 API:

Example using s3cmd to test AWS S3:

1. Create config file:
```
[default]
access_key = test:tester
secret_key = testing
host_base = localhost:8080
host_bucket = localhost:8080
use_https = False
```

2. Test with s3cmd:
```
s3cmd -c s3cfg_saio mb s3://bucket
```

# Quick Reference

- **Image tags**: `latest` automatically built/published by Zuul, follows
   master branch. Releases are also tagged in case you want to test against
   a specific release.
- **Source Code**: github.com/openstack/swift
- **Maintained by**: Openstack Swift community
- **Feedback/Questions**: #openstack-swift on OFTC
