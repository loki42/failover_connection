failover_connection
===================

This is a redis connection with failover in the event of a connection error, the connection pool is managed with zookeeper.

In the event of an connection error, the first connection gets a write lock and elects a new master. Other connections wait until the write lock is released to get the new master.

This is the inital version and it's probably very broken. A number of things are not yet handled properly.

I'll do a write up soon about ZooKeeper on the Gravity Four blog.
