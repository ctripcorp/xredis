start_server {tags {"pubsub network"}} {
    ### Keyspace events notification tests

    test "Keyspace notifications: we receive keyspace notifications" {
        r config set notify-keyspace-events KA
        set rd1 [redis_deferring_client]
        assert_equal {1} [psubscribe $rd1 *]
        r set foo bar
        assert_match {pmessage * __keyspace@*__:foo set} [$rd1 read]
        $rd1 close
    }

    test "Keyspace notifications: we receive keyevent notifications" {
        r config set notify-keyspace-events EA
        set rd1 [redis_deferring_client]
        assert_equal {1} [psubscribe $rd1 *]
        r set foo bar
        assert_match {pmessage * __keyevent@*__:set foo} [$rd1 read]
        $rd1 close
    }

    test "Keyspace notifications: we can receive both kind of events" {
        r config set notify-keyspace-events KEA
        set rd1 [redis_deferring_client]
        assert_equal {1} [psubscribe $rd1 *]
        r set foo bar
        assert_match {pmessage * __keyspace@*__:foo set} [$rd1 read]
        assert_match {pmessage * __keyevent@*__:set foo} [$rd1 read]
        $rd1 close
    }

    test "Keyspace notifications: we are able to mask events" {
        r config set notify-keyspace-events KEl
        r del mylist
        set rd1 [redis_deferring_client]
        assert_equal {1} [psubscribe $rd1 *]
        r set foo bar
        r lpush mylist a
        # No notification for set, because only list commands are enabled.
        assert_match {pmessage * __keyspace@*__:mylist lpush} [$rd1 read]
        assert_match {pmessage * __keyevent@*__:lpush mylist} [$rd1 read]
        $rd1 close
    }

    test "Keyspace notifications: general events test" {
        r config set notify-keyspace-events KEg
        set rd1 [redis_deferring_client]
        assert_equal {1} [psubscribe $rd1 *]
        r set foo bar
        r expire foo 1
        r del foo
        assert_match {pmessage * __keyspace@*__:foo expire} [$rd1 read]
        assert_match {pmessage * __keyevent@*__:expire foo} [$rd1 read]
        assert_match {pmessage * __keyspace@*__:foo del} [$rd1 read]
        assert_match {pmessage * __keyevent@*__:del foo} [$rd1 read]
        $rd1 close
    }

    test "Keyspace notifications: list events test" {
        r config set notify-keyspace-events KEl
        r del mylist
        set rd1 [redis_deferring_client]
        assert_equal {1} [psubscribe $rd1 *]
        r lpush mylist a
        r rpush mylist a
        r rpop mylist
        assert_match {pmessage * __keyspace@*__:mylist lpush} [$rd1 read]
        assert_match {pmessage * __keyevent@*__:lpush mylist} [$rd1 read]
        assert_match {pmessage * __keyspace@*__:mylist rpush} [$rd1 read]
        assert_match {pmessage * __keyevent@*__:rpush mylist} [$rd1 read]
        assert_match {pmessage * __keyspace@*__:mylist rpop} [$rd1 read]
        assert_match {pmessage * __keyevent@*__:rpop mylist} [$rd1 read]
        $rd1 close
    }

    test "Keyspace notifications: set events test" {
        r config set notify-keyspace-events Ks
        r del myset
        set rd1 [redis_deferring_client]
        assert_equal {1} [psubscribe $rd1 *]
        r sadd myset a b c d
        r srem myset x
        r sadd myset x y z
        r srem myset x
        assert_match {pmessage * __keyspace@*__:myset sadd} [$rd1 read]
        assert_match {pmessage * __keyspace@*__:myset sadd} [$rd1 read]
        assert_match {pmessage * __keyspace@*__:myset srem} [$rd1 read]
        $rd1 close
    }

    test "Keyspace notifications: zset events test" {
        r config set notify-keyspace-events Kz
        r del myzset
        set rd1 [redis_deferring_client]
        assert_equal {1} [psubscribe $rd1 *]
        r zadd myzset 1 a 2 b
        r zrem myzset x
        r zadd myzset 3 x 4 y 5 z
        r zrem myzset x
        assert_match {pmessage * __keyspace@*__:myzset zadd} [$rd1 read]
        assert_match {pmessage * __keyspace@*__:myzset zadd} [$rd1 read]
        assert_match {pmessage * __keyspace@*__:myzset zrem} [$rd1 read]
        $rd1 close
    }

    test "Keyspace notifications: hash events test" {
        r config set notify-keyspace-events Kh
        r del myhash
        set rd1 [redis_deferring_client]
        assert_equal {1} [psubscribe $rd1 *]
        r hmset myhash yes 1 no 0
        r hincrby myhash yes 10
        assert_match {pmessage * __keyspace@*__:myhash hset} [$rd1 read]
        assert_match {pmessage * __keyspace@*__:myhash hincrby} [$rd1 read]
        $rd1 close
    }

    test "Keyspace notifications: expired events (triggered expire)" {
        r config set notify-keyspace-events Ex
        r del foo
        set rd1 [redis_deferring_client]
        assert_equal {1} [psubscribe $rd1 *]
        r psetex foo 100 1
        wait_for_condition 50 100 {
            [r exists foo] == 0
        } else {
            fail "Key does not expire?!"
        }
        assert_match {pmessage * __keyevent@*__:expired foo} [$rd1 read]
        $rd1 close
    }

    test "Keyspace notifications: expired events (background expire)" {
        r config set notify-keyspace-events Ex
        r del foo
        set rd1 [redis_deferring_client]
        assert_equal {1} [psubscribe $rd1 *]
        r psetex foo 100 1
        assert_match {pmessage * __keyevent@*__:expired foo} [$rd1 read]
        $rd1 close
    }

}
