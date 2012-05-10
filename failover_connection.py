#
# Copyright (c) 2012 Loki Davison for Gravity Four
#
# This is a redis connection with failover in the event of a connection error,
# the pool of servers is kept in ZooKeeper. This is the initial version and very untested.
#
# This is free software; you can redistribute it and/or modify it under
# the terms of the Lesser GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# failover_connection is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# Lesser GNU General Public License for more details.
#
# You should have received a copy of the Lesser GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.


from redis.connection import Connection as BaseConnection
from redis.exceptions import ConnectionError
import redis
import logging
from zktools.locking import ZkReadLock, ZkWriteLock

logger = logging.getLogger(__name__)

"""
import zc.zk
import failover_connection
import redis
zk = zc.zk.ZooKeeper('localhost:2181')
pool = redis.ConnectionPool(connection_class=cxn.FailoverConnection, zk_client=zk)
r = redis.Redis(connection_pool=pool)
r.hgetall(2)

"""

class FailoverConnection(BaseConnection):
    """
    A Redis connection where the pool of servers is kept in ZooKeeper
    """

    def __init__(self, zk_client, max_attempts=3, **kwargs):
        print "kw args are", kwargs
        self.zk = zk_client
        self.host = None
        self.port = None
        master = self.zk.get_properties("/redis/master")
        if master:
            host, port = master['address'].split(':')
        else:
            master = self.elect_master()
            host, port = master['address'].split(':')

        kwargs['host'] = host
        kwargs['port'] = int(port)
        BaseConnection.__init__(self, **kwargs)
        self.max_attempts = max_attempts

    def connect(self):
        # get current leader from zookeeper,
        # if no current leader, elect one.
        for i in range(self.max_attempts):
            try:
                return BaseConnection.connect(self)
            except ConnectionError, e:
                #print "connection error", e
                # if we've failed max_attempts times,
                if i == self.max_attempts-1:
                    # check if someone else has already updated the master
                    master = self.zk.get_properties("/redis/master")
                    host, port = master['address'].split(':')
                    port = int(port)
                    if host == self.host and port == self.port:
                        # if not remove this master and elect a new one.
                        master = self.elect_master()
                        #print "master is ", master
                        if master is False:
                            raise
                        host, port = master['address'].split(':')
                        port = int(port)
                        # update and recure
                    self.update(host=host, port=port)
                    return self.connect()

    def elect_master(self):
        zk = self.zk
        write_lock = ZkWriteLock(zk, "redis_leader")
        if write_lock.acquire(timeout=0): # get if we can write or not
            # if the host/port is the same as our current connection or none
            #print "acquired write lock"
            data = zk.properties('/redis/master')
            if 'address' in data.keys():
                host, port = data['address'].split(':')
                port = int(port)
            # master isn't set or host / port is same as current.
            if (not 'address' in data.keys()) or (host == self.host and port == self.port):
                # if master is set, remove it.
                if 'address' in data.keys():
                    zk.delete('/redis/providers/'+data['address'])
                addresses = zk.get_children('/redis/providers')
                master = addresses[0]
                # set master as slave of no one
                host, port = master.split(':')
                port = int(port)
                r = redis.StrictRedis(host=host, port=port, db=0)
                r.slaveof() # set to be master.
                # set all the others as slaves of the new master
                for address in addresses[1:]:
                    c_host, c_port = address.split(':')
                    c_port = int(c_port)
                    r = redis.StrictRedis(host=c_host, port=c_port, db=0)
                    r.slaveof(host=host, port=port)
                data.set(address=master)
                master = {'address': master}
            else:
                master = data
            write_lock.release()
        else:
            #wait for other elector to decide on who is master and release the write locking
            #print "can't get a write lock, waiting to read"
            read_lock = ZkReadLock(zk, "redis_leader")
            with read_lock(): # blocking acquire
                master = zk.get_properties("/redis/master")
                #print "read master"
        return master

    def update(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)
        self._sock = None
