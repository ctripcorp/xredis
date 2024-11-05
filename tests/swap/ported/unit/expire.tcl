start_server {tags {"expire"}} {
    test {5 keys in, 5 keys out} {
        r flushdb
        r set a c
        r expire a 5
        r set t c
        r set e c
        r set s c
        r set foo b
        wait_keyspace_cold r
        r scan 0
        lsort [lindex [r scan 1] 1]
    } {a e foo s t}
}
