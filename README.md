failover_connection
===================

This is a redis connection with failover in the event of a connection error, the connection pool is managed with zookeeper.

In the event of an connection error, the first connection gets a write lock and elects a new master. Other connections wait until the write lock is released to get the new master.

This is the inital version and it's probably very broken. A number of things are not yet handled properly.

I orignally wrote this using zc.zk and zktools. The currently don't work with gevent though so i've rewritten it to use kazoo. I've tested it with and without gvent and currently
use it with both. 

I'll do a write up soon about ZooKeeper on the Gravity Four blog.
