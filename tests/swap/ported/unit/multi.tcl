start_server {tags {"multi"}} {

    # NOTE in swap mode, we don't know the whole keyspace, so we assume
    # that all key is touched and affected
    test {FLUSHALL *does* touch non affected keys} {
        r del x
        r watch x
        r flushall
        r multi
        r ping
        r exec
    } {}

    test {FLUSHDB is able to touch the watched keys} {
        r set x 30
        r watch x
        r flushdb
        r multi
        r ping
        r exec
    } {}

    test {FLUSHDB *does* touch non affected keys} {
        r del x
        r watch x
        r flushdb
        r multi
        r ping
        r exec
    } {}

}
