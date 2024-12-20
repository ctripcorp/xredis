start_server {tags {"swap.info"} overrides {swap-swap-info-propagate-mode {swap.info} }}  {
    test "SST-AGE-LIMIT" {
        r swap.info SST-AGE-LIMIT 1111
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit 1111

        r swap.info SST-AGE-LIMIT 0
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit 0

        r swap.info SST-AGE-LIMIT -2222
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit -2222
    }

    test "no error for illegal swap.info cmd" {
        assert_equal OK [r swap.info illegal]
        assert_equal OK [r swap.info SST-AGE-LIMIT]
        assert_equal OK [r swap.info SST-AGE-LIMIT 50 50]
        assert_equal OK [r swap.info SST-AGE-LIMIT 50 50 50 err]
    }
}

start_server {tags {"swap.info"} overrides {swap-swap-info-propagate-mode {ping} }}  {
    test "SST-AGE-LIMIT with ping" {
        r ping "swap.info SST-AGE-LIMIT 1111"
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit 1111

        r ping "swap.info SST-AGE-LIMIT 0"
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit 0

        r ping "swap.info SST-AGE-LIMIT -2222"
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit -2222
    }

    test "no error for illegal ping argv of swap.info" {

        r ping "swap.info SST-AGE-LIMIT 666"

        assert_equal "swap.info SST-AGE 111" [r ping "swap.info SST-AGE 111"]
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit 666

        assert_equal "swap.info SST-AGE" [r ping "swap.info SST-AGE"]
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit 666

        assert_equal "swap.info" [r ping "swap.info"]
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit 666

        assert_equal "swap.info ss ss ss ss" [r ping "swap.info ss ss ss ss"]
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit 666

        assert_equal "swap.info  " [r ping "swap.info  "]
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit 666
    }
}