
start_server {tags {"repl"}} {
    r config set swap-debug-evict-keys 0
    set types {string hash set zset list keyspace transaction scripting}
    set type_add_commands {}
    dict set type_add_commands string {
        r set string v 
        r set string1 v
    }
    dict set type_add_commands hash {
        r hset hash k v k1 v1
        r hset hash1 k v k1 v1
    }
    dict set type_add_commands set {
        r sadd set k k1 
        r sadd set1 k k1
    }
    dict set type_add_commands zset {
        r zadd zset 10 k 20 k1 
        r zadd zset1 10 k 20 k1
    }
    dict set type_add_commands list {
        r lpush list k k1
        r lpush list1 k k1
    }
    dict set type_add_commands keyspace {
        randpath {
            r set keyspace v 
            r set keyspace1 v
        } {
            r hset keyspace k v k1 v1 
            r hset keyspace1 k v k1 v1 
        } {
            r sadd keyspace k k1 
            r sadd keyspace1 k k1 
        } {
            r zadd keyspace 10 k 20 k1 
            r zadd keyspace1 10 k 20 k1 
        }
    }

    dict set type_add_commands transaction {
        randpath {
            r set transaction v 
            r set transaction1 v
        } {
            r hset transaction k v k1 v1 
            r hset transaction1 k v k1 v1 
        } {
            r sadd transaction k k1 
            r sadd transaction1 k k1 
        } {
            r zadd transaction 10 k 20 k1 
            r zadd transaction1 10 k 20 k1 
        }
    }

    dict set type_add_commands scripting {
        randpath {
            r set scripting v 
            r set scripting1 v
        } {
            r hset scripting k v k1 v1 
            r hset scripting1 k v k1 v1 
        } {
            r sadd scripting k k1 
            r sadd scripting1 k k1 
        } {
            r zadd scripting 10 k 20 k1 
            r zadd scripting1 10 k 20 k1 
        }
    }
    
    set type_commands {}
    #string commands  
    set string_commands {}
    lappend string_commands {
        return [r get $key]
    }
    lappend string_commands {
        return [r getex $key]
    }
    lappend string_commands {
        return [r getdel $key]
    }
    lappend string_commands {
        return [r set $key v]
    }
    lappend string_commands {
        return [r setnx $key v]
    }
    lappend string_commands {
        return [r setex  $key 100 v]
    }
    lappend string_commands {
        return [r psetex  $key 10000 v]
    }
    lappend string_commands {
        return [r append $key v]
    }
    lappend string_commands {
        return [r strlen $key]
    }
    
    lappend string_commands {
        return [r setrange $key 0 hello]
    }
    lappend string_commands {
        return [r getrange $key 0 100]
    }
    lappend string_commands {
        return [r substr $key 0 100]
    }
    lappend string_commands {
        return [r incr $key]
    }
    lappend string_commands {
        return [r decr $key]
    }

    lappend string_commands {
        return [r mget $key]
    }
    lappend string_commands {
        return [r incrby $key 1]
    }
    lappend string_commands {
        return [r decrby $key 1]
    }
    lappend string_commands {
        return [r incrbyfloat $key 1]
    }
    lappend string_commands {
        return [r getset $key v]
    }
    lappend string_commands {
        return [r mset $key v ]
    }
    lappend string_commands {
        return [r msetnx $key v]
    }
    #bitmap
    lappend string_commands {
        return [r setbit $key 0 1]
    }
    lappend string_commands {
        return [r getbit $key 0]
    }
    lappend string_commands {
        return [r bitfield $key get i8 0]
    }
    lappend string_commands {
        return [r bitfield $key set i8 0 120]
    }
    lappend string_commands {
        return [r bitfield_ro $key get i8 0]
    }
    # hyperloglog
    lappend string_commands {
        return [r pfadd $key a ]
    }
    lappend string_commands {
        return [r pfcount $key]
    }
    lappend string_commands {
        return [r pfmerge string4 $key string3]
    }


    dict set type_commands string $string_commands
    set hash_commands {}
    lappend hash_commands {
        return [r hset $key k v]
    }
    lappend hash_commands {
        return [r hsetnx $key k v]
    }
    lappend hash_commands {
        return [r hget $key k ]
    }
    lappend hash_commands {
        return [r hmget $key k ]
    }
    lappend hash_commands {
        return [r hlen $key]
    }
    lappend hash_commands {
        return [r hincrby $key k 10]
    }
    lappend hash_commands {
        return [r hincrbyfloat $key k 10]
    }
    lappend hash_commands {
        return [r hdel $key k]
    }
    lappend hash_commands {
        return [r hstrlen $key k]
    }
    lappend hash_commands {
        return [r hmset $key k v]
    }
    lappend hash_commands {
        return [r hkeys $key]
    }
    lappend hash_commands {
        return [r hvals $key]
    }
    lappend hash_commands {
        return [r hgetall $key]
    }
    lappend hash_commands {
        return [r hexists $key k]
    }
    lappend hash_commands {
        return [r hrandfield $key]
    }
    lappend hash_commands {
        return [r hscan $key 0]
    }
    lappend hash_commands {
        return [r hdel $key k]
    }
    dict set type_commands hash $hash_commands
    set set_commands {}
    lappend set_commands {
        return [r sadd $key k2]
    }
    lappend set_commands {
        return [r srem $key k]
    }
    lappend set_commands {
        return [r smove $key set3 k]
    }
    lappend set_commands {
        return [r sismember $key k]
    }
    lappend set_commands {
        return [r smismember $key k]
    }
    lappend set_commands {
        return [r scard $key]
    }
    lappend set_commands {
        return [r spop $key]
    }
    lappend set_commands {
        return [r srandmember $key]
    }
    lappend set_commands {
        return [r sinter $key set3]
    }
    lappend set_commands {
        return [r sinterstore $key set3]
    }
    lappend set_commands {
        return [r sunion $key set3]
    }
    lappend set_commands {
        return [r sunionstore $key set3]
    }
    lappend set_commands {
        return [r sdiff $key set3]
    }
    lappend set_commands {
        return [r sdiffstore $key set3]
    }
    lappend set_commands {
        return [r smembers $key ]
    }
    lappend set_commands {
        return [r sscan $key 0]
    }
    


    dict set type_commands set $set_commands 
    set zset_commands {}
    lappend zset_commands {
        return [r zadd $key 10 k3]
    }
    lappend zset_commands {
        return [r zincrby $key 10 k]
    }
    lappend zset_commands {
        return [r zrem $key k]
    }
    lappend zset_commands {
        return [r zremrangebyscore $key 10 20]
    }
    lappend zset_commands {
        return [r zremrangebyrank $key 1 -1]
    }
    lappend zset_commands {
        return [r zremrangebylex $key "\[a" "\[z" ]
    }
    lappend zset_commands {
        return [r zunionstore zset4 2 $key zset3 weights 2 3]
    }
    lappend zset_commands {
        return [r zinterstore zset4 2 $key zset3 weights 2 3]
    }
    lappend zset_commands {
        return [r zdiffstore zset4 2 $key zset3]
    }
    lappend zset_commands {
        return [r zunion 2 $key zset3]
    }
    lappend zset_commands {
        return [r zinter 2 $key zset3]
    }
    lappend zset_commands {
        return [r zdiff 2 $key zset3]
    }
    lappend zset_commands {
        return [r zrange $key 0 1]
    }
    lappend zset_commands {
        return [r zrangestore zset3 $key 0 1]
    }
    lappend zset_commands {
        return [r zrangebyscore $key 0 100]
    }
    lappend zset_commands {
        return [r zrevrangebyscore $key 0 100]
    }
    lappend zset_commands {
        return [r zrangebylex $key "\[a" "\[z"]
    }
    lappend zset_commands {
        return [r zrevrangebylex $key "\[a" "\[z"]
    }
    lappend zset_commands {
        return [r zcount $key 0 100]
    }
    lappend zset_commands {
        return [r zlexcount $key "\[a" "\[z"]
    }
    lappend zset_commands {
        return [r zrevrange $key 0 100]
    }
    lappend zset_commands {
        return [r zcard $key]
    }
    lappend zset_commands {
        return [r zscore $key k]
    }
    lappend zset_commands {
        return [r zmscore $key k]
    }
    lappend zset_commands {
        return [r zrank $key k]
    }
    lappend zset_commands {
        return [r zrevrank $key k]
    }
    lappend zset_commands {
        return [r zscan $key 0]
    }
    lappend zset_commands {
        return [r zpopmin $key]
    }
    lappend zset_commands {
        return [r zpopmax $key]
    }
    lappend zset_commands {
        return [r bzpopmin $key 1]
    }
    lappend zset_commands {
        return [r bzpopmax $key 1]
    }
    lappend zset_commands {
        return [r zrandmember $key]
    }
    #geo
    lappend zset_commands {
        return [r GEOADD $key 1.0 1.0 k]
    }
    lappend zset_commands {
        return [r georadius $key 0 0 3 km]
    }
    lappend zset_commands {
        return [r georadius_ro $key 0 0 3 km]
    }
    lappend zset_commands {
        return [r georadiusbymember $key k 100 km]
    }
    lappend zset_commands {
        return [r georadiusbymember_ro $key k 100 km]
    }
    lappend zset_commands {
        return [r GEOHASH $key k]
    }
    lappend zset_commands {
        return [r geopos $key k]
    }
    lappend zset_commands {
        return [r geodist $key k k1]
    }
    lappend zset_commands {
        return [r geosearch $key FROMLONLAT 0 0 BYRADIUS 10 km ASC]
    }
    lappend zset_commands {
        return [r geosearchstore zset3 $key FROMLONLAT 0 0 BYRADIUS 10 km ASC]
    }










    dict set type_commands zset $zset_commands 
    set list_commands {}
    lappend list_commands {
        return [r lpush $key k3]
    }
    lappend list_commands {
        return [r rpush $key k3]
    }
    lappend list_commands {
        return [r rpushx $key k3]
    }
    lappend list_commands {
        return [r lpushx $key k3]
    }
    lappend list_commands {
        return [r linsert $key BEFORE k k3]
    }
    lappend list_commands {
        return [r rpop $key]
    }
    lappend list_commands {
        return [r lpop $key]
    }
    lappend list_commands {
        return [r brpop $key 1]
    }
    lappend list_commands {
        return [r brpoplpush $key list3 1]
    }
    lappend list_commands {
        return [r blmove $key list3 left left 1]
    }
    lappend list_commands {
        return [r blpop $key 1]
    }
    lappend list_commands {
        return [r llen $key]
    }
    lappend list_commands {
        return [r lindex $key 0]
    }
    lappend list_commands {
        return [r lset $key 0 k3]
    }
    lappend list_commands {
        return [r lrange $key 0 1]
    }
    lappend list_commands {
        return [r ltrim $key 0 1]
    }
    lappend list_commands {
        return [r lpos $key k]
    }
    lappend list_commands {
        return [r lrem $key 0 k]
    }
    lappend list_commands {
        return [r rpoplpush $key list3]
    }
    lappend list_commands {
        return [r lmove $key list3 left left]
    }


    dict set type_commands list $list_commands 
    
    set keyspace_commands {}
    lappend keyspace_commands {
        return [r del $key]
    }
    lappend keyspace_commands {
        return [r unlink $key]
    }
    lappend keyspace_commands {
        return [r ttl $key]
    }
    lappend keyspace_commands {
        return [r type $key]
    }
    # lappend keyspace_commands {
    #     return [r dump $key]
    # }
    # lappend keyspace_commands {
    #     return [r object $key]
    # }

    dict set type_commands keyspace $keyspace_commands
    
    set transaction_commands {}
    lappend transaction_commands {
        return [r watch $key]
    }
    lappend transaction_commands {
        return [r unwatch]
    }
    lappend transaction_commands {
        r multi
        r set $key v
        return [r exec]
    }
    dict set type_commands transaction $transaction_commands 
    set scripting_commands {}
    lappend scripting_commands {
        r eval "return redis.call('set', KEYS[1], 'v');" 1 $key
    }
    dict set type_commands scripting $scripting_commands 
    foreach type $types {
        foreach command_type $types {
            if {$type == $command_type} {
                continue
            } 
            foreach command [dict get $type_commands $command_type] {
                test [format "%s - exec %s" $type [string trim $command] ] {
                    set key1 [format "%s1" $type]
                    proc exec_command {key} $command
                    # set data 
                    while {1} {
                        uplevel 1 [dict get $type_add_commands $type]
                        if {[r type $type] == $command_type} {
                            # keyspace random type
                            r del $type 
                            r del $key1
                        } else {
                            break
                        }
                    }
                    # evict 
                    r swap.evict $type 
                    wait_key_cold r $type
                    # get data 
                    set error ""
                    set error2 ""
                    set result [catch {exec_command $type} error] 
                    set result2 [catch {exec_command $key1} error2]
                    assert_equal $error $error2
                    assert_equal $result $result2
                    r del $type 
                    r del $key1
                }
            }
            
        }
    }
    
}
