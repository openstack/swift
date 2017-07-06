==============================
Introduction to Object Storage
==============================

OpenStack Object Storage (swift) is used for redundant, scalable data
storage using clusters of standardized servers to store petabytes of
accessible data. It is a long-term storage system for large amounts of
static data which can be retrieved and updated. Object Storage uses a
distributed architecture
with no central point of control, providing greater scalability,
redundancy, and permanence. Objects are written to multiple hardware
devices, with the OpenStack software responsible for ensuring data
replication and integrity across the cluster. Storage clusters scale
horizontally by adding new nodes. Should a node fail, OpenStack works to
replicate its content from other active nodes. Because OpenStack uses
software logic to ensure data replication and distribution across
different devices, inexpensive commodity hard drives and servers can be
used in lieu of more expensive equipment.

Object Storage is ideal for cost effective, scale-out storage. It
provides a fully distributed, API-accessible storage platform that can
be integrated directly into applications or used for backup, archiving,
and data retention.
