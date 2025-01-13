proc cmdstat {cmd} {
    return [cmdrstat $cmd r]
}

start_server {tags {"introspection"}} {
    test {command stats for scripts} {
        r config resetstat
        r set mykey myval
        r eval {
            redis.call('set', KEYS[1], 0)
            redis.call('expire', KEYS[1], 0)
            redis.call('geoadd', KEYS[2], 0, 0, "bar")
        } 2 mykey mykey2
        assert_match {*calls=1,*} [cmdstat eval]
        assert_match {*calls=2,*} [cmdstat set]
        assert_match {*calls=1,*} [cmdstat expire]
        assert_match {*calls=1,*} [cmdstat geoadd]
    }
}
