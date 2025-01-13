
start_server {
    tags {"list"}
    overrides {
        "list-max-ziplist-size" 5
    }
} {
    source "tests/unit/type/list-common.tcl"

    test "Linked LMOVEs" {
      set rd1 [redis_deferring_client]
      set rd2 [redis_deferring_client]

      r del list1 list2 list3

      $rd1 blmove list1 list2 right left 0
      $rd2 blmove list2 list3 left right 0

      r rpush list1 foo
      assert_equal foo [$rd1 read]
      assert_equal foo [$rd2 read]

      assert_equal {} [r lrange list1 0 -1]
      assert_equal {} [r lrange list2 0 -1]
      assert_equal {foo} [r lrange list3 0 -1]
    }

    test "BRPOPLPUSH does not affect WATCH while still blocked" {
        set blocked_client [redis_deferring_client]
        set watching_client [redis_deferring_client]
        r del srclist dstlist somekey
        r set somekey somevalue
        $blocked_client brpoplpush srclist dstlist 0
        $watching_client watch dstlist
        $watching_client read
        $watching_client multi
        $watching_client read
        $watching_client get somekey
        $watching_client read
        $watching_client exec
        # Blocked BLPOPLPUSH may create problems, unblock it.
        after 100
        r lpush srclist element
        $watching_client read
    } {somevalue}
}
