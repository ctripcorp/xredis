start_server {
    tags {"set"}
} {
    test {pure hot} {
        setbit tbitmap1 40960 1
        swap.evict tbitmap1
        assert_equal {1} [r getbit tbitmap1 40960]
        setbit tbitmap1 40960 0
        swap.evict tbitmap1
        assert_equal {0} [r getbit tbitmap1 40960]
        del tbitmap1
        assert_equal {0} [r getbit tbitmap1 40960]
    }
}