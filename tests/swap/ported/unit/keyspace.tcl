start_server {tags {"keyspace"}} {



    #note refcount may be 2 (increased by swapdata) if current
    #data is hot/warm or not supports swap, may be 1 if current
    #data is cold and supports swap. */
    test {COPY basic usage for list} {
        r del mylist mynewlist
        r lpush mylist a b c d
        r copy mylist mynewlist
        set digest [r debug digest-value mylist]
        assert_equal $digest [r debug digest-value mynewlist]
        set mylist_ref [r object refcount mylist]
        set mynewlist_ref [r object refcount mynewlist]
        assert {$mylist_ref==1 || $mylist_ref==2}
        assert {$mynewlist_ref==1 || $mynewlist_ref==2}
        r del mylist
        assert_equal $digest [r debug digest-value mynewlist]
    }

    test {COPY basic usage for intset set} {
        r del set1 newset1
        r sadd set1 1 2 3
        assert_encoding intset set1
        r copy set1 newset1
        set digest [r debug digest-value set1]
        assert_equal $digest [r debug digest-value newset1]
        set set1_ref [r object refcount set1]
        set newset1_ref [r object refcount newset1]
        assert {$set1_ref==1 || $set1_ref==2}
        assert {$newset1_ref==1 || $newset1_ref==2}
        r del set1
        assert_equal $digest [r debug digest-value newset1]
    }

    test {COPY basic usage for hashtable set} {
        r del set2 newset2
        r sadd set2 1 2 3 a
        assert_encoding hashtable set2
        r copy set2 newset2
        set digest [r debug digest-value set2]
        assert_equal $digest [r debug digest-value newset2]
        set set2_ref [r object refcount set2]
        set newset2_ref [r object refcount newset2]
        assert {$set2_ref==1 || $set2_ref==2}
        assert {$newset2_ref==1 || $newset2_ref==2}
        r del set2
        assert_equal $digest [r debug digest-value newset2]
    }

    test {COPY basic usage for ziplist sorted set} {
        r del zset1 newzset1
        r zadd zset1 123 foobar
        assert_encoding ziplist zset1
        r copy zset1 newzset1
        set digest [r debug digest-value zset1]
        assert_equal $digest [r debug digest-value newzset1]
        set zset1_ref [r object refcount zset1]
        set newset1_ref [r object refcount newzset1]
        assert {$zset1_ref==1 || $zset1_ref==2}
        assert {$newset1_ref==1 || $newset1_ref==2}
        r del zset1
        assert_equal $digest [r debug digest-value newzset1]
    }

     test {COPY basic usage for skiplist sorted set} {
        r del zset2 newzset2
        set original_max [lindex [r config get zset-max-ziplist-entries] 1]
        r config set zset-max-ziplist-entries 0
        for {set j 0} {$j < 130} {incr j} {
            r zadd zset2 [randomInt 50] ele-[randomInt 10]
        }
        assert_encoding skiplist zset2
        r copy zset2 newzset2
        set digest [r debug digest-value zset2]
        assert_equal $digest [r debug digest-value newzset2]
        set zset2_ref [r object refcount zset2]
        set newset2_ref [r object refcount newzset2]
        assert {$zset2_ref==1 || $zset2_ref==2}
        assert {$newset2_ref==1 || $newset2_ref==2}
        r del zset2
        assert_equal $digest [r debug digest-value newzset2]
        r config set zset-max-ziplist-entries $original_max
    }

    test {COPY basic usage for stream} {
        r del mystream mynewstream
        for {set i 0} {$i < 1000} {incr i} {
            r XADD mystream * item 2 value b
        }
        r copy mystream mynewstream
        set digest [r debug digest-value mystream]
        assert_equal $digest [r debug digest-value mynewstream]
        assert_equal 2 [r object refcount mystream]
        assert_equal 2 [r object refcount mynewstream]
        r del mystream
        assert_equal $digest [r debug digest-value mynewstream]
    }

    test {COPY basic usage for stream-cgroups} {
        r del x
        r XADD x 100 a 1
        set id [r XADD x 101 b 1]
        r XADD x 102 c 1
        r XADD x 103 e 1
        r XADD x 104 f 1
        r XADD x 105 g 1
        r XGROUP CREATE x g1 0
        r XGROUP CREATE x g2 0
        r XREADGROUP GROUP g1 Alice COUNT 1 STREAMS x >
        r XREADGROUP GROUP g1 Bob COUNT 1 STREAMS x >
        r XREADGROUP GROUP g1 Bob NOACK COUNT 1 STREAMS x >
        r XREADGROUP GROUP g2 Charlie COUNT 4 STREAMS x >
        r XGROUP SETID x g1 $id
        r XREADGROUP GROUP g1 Dave COUNT 3 STREAMS x >
        r XDEL x 103

        r copy x newx
        set info [r xinfo stream x full]
        assert_equal $info [r xinfo stream newx full]
        assert_equal 2 [r object refcount x]
        assert_equal 2 [r object refcount newx]
        r del x
        assert_equal $info [r xinfo stream newx full]
        r flushdb
    }

}
