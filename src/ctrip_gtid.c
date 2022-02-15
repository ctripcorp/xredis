#include "server.h"
#include "sds.h"
#include "zmalloc.h"

gtidInterval *gtidIntervalNew(rpl_gno gno) {
    gtidInterval *interval = zmalloc(sizeof(*interval));
    interval->gno_start = gno;
    interval->gno_end = gno;
    interval->next = NULL;
    return interval;
}

uuidSet *uuidSetNew(const char* rpl_sid, rpl_gno gno) {
    uuidSet *uuid_set = zmalloc(sizeof(*uuid_set));
    uuid_set->rpl_sid = sdsnew(rpl_sid);
    uuid_set->intervals = gtidIntervalNew(gno);
    return uuid_set;
}

void uuidSetFree(uuidSet *uuid_set) {
    sdsfree(uuid_set->rpl_sid);
    gtidInterval *cur = uuid_set->intervals;
    while(cur != NULL) {
        gtidInterval *next = cur->next;
        zfree(cur);
        cur = next;
    }
    zfree(uuid_set);
}

int uuidSetAdd(uuidSet *uuid_set, rpl_gno gno) {
    gtidInterval *cur = uuid_set->intervals;
    gtidInterval *next = cur->next;
    if (gno < cur->gno_start - 1) {
        uuid_set->intervals = gtidIntervalNew(gno);
        uuid_set->intervals->next = cur;
        return 1;
    }
    if (gno == cur->gno_start - 1) {
        cur->gno_start = gno;
        return 1;
    }
    while(next != NULL) {
        if (gno == cur->gno_end + 1) {
            if (gno == next->gno_start - 1) {
                cur->gno_end = next->gno_end;
                cur->next = next->next;
                zfree(next);
                return 1;
            } else {
                cur->gno_end = gno;
                return 1;
            }
        }
        if (gno == next->gno_start - 1) {
            next->gno_start = gno;
            return 1;
        }
        if (gno < next->gno_start - 1 && gno > cur->gno_end + 1) {
            cur->next = gtidIntervalNew(gno);
            cur->next->next = next;
            return 1;
        }
        if (gno < next->gno_end) {
            return 0;
        }
        cur = next;
        next = cur->next;
    }
    if (gno == cur->gno_end + 1) {
        cur->gno_end = gno;
        return 1;
    }
    cur->next = gtidIntervalNew(gno);
    return 1;
}

gtidSet* gtidSetNew() {
    gtidSet *gtid_set = zmalloc(sizeof(*gtid_set));
    gtid_set->uuid_sets = NULL;
    return gtid_set;
}

sds gtidEncode(gtidSet *gtid_set) {
    if (NULL == gtid_set->uuid_sets) {
        return sdsempty();
    }
    return sdsnew("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA:1-7");
}

gtidSet* gtidAdd(gtidSet *gtid_set, const char *gtid) {

    return NULL;
}

gtidSet* gtidRaise(gtidSet *gtid_set, const char *rpl_sid, rpl_gno watermark) {

    return NULL;
}

#if defined(GTID_TEST_MAIN)
#include <stdio.h>
#include "testhelp.h"
#include "limits.h"

#define UNUSED(x) (void)(x)
int gtidTest(void) {
    {
        /* gtid unit tests*/

        gtidInterval *interval = gtidIntervalNew(9);
        test_cond("Create an new gtid interval with 9",
            interval->gno_start == 9 && interval->gno_end == 9);

        zfree(interval);

        gtidSet *gtid_set = gtidSetNew();

        test_cond("Create an empty gtid set",
            memcmp(gtidEncode(gtid_set), "\0", 1) == 0);


        /* uuid unit tests*/
        uuidSet *uuid_set;

        uuid_set = uuidSetNew("A", 9);

        test_cond("Create an new uuid set with 9",
            uuid_set->intervals->gno_start == 9 && uuid_set->intervals->gno_end == 9
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetAdd(uuid_set, 7);

        test_cond("Add 7 to 9",
            uuid_set->intervals->gno_start == 7 && uuid_set->intervals->gno_end == 7
            && uuid_set->intervals->next->gno_start == 9 && uuid_set->intervals->next->gno_end == 9
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);

        uuid_set = uuidSetNew("A", 9);
        uuidSetAdd(uuid_set, 8);

        test_cond("Add 8 to 9",
            uuid_set->intervals->gno_start == 8 && uuid_set->intervals->gno_end == 9
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetAdd(uuid_set, 6);

        test_cond("Add 6 to 8-9",
            uuid_set->intervals->gno_start == 6 && uuid_set->intervals->gno_end == 6
            && uuid_set->intervals->next->gno_start == 8 && uuid_set->intervals->next->gno_end == 9
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        test_cond("Add 8 to 8-9",
            uuidSetAdd(uuid_set, 8) == 0);

        uuidSetAdd(uuid_set, 7);

        test_cond("Add 7 to 6,8-9",
            uuid_set->intervals->gno_start == 6 && uuid_set->intervals->gno_end == 9
            && uuid_set->intervals->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetAdd(uuid_set, 100);
        test_cond("Add 100 to 6-9",
            uuid_set->intervals->gno_start == 6 && uuid_set->intervals->gno_end == 9
            && uuid_set->intervals->next->gno_start == 100 && uuid_set->intervals->next->gno_end == 100
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);

        uuid_set = uuidSetNew("A", 0);
        uuidSetAdd(uuid_set, 5);
        uuidSetAdd(uuid_set, 6);
        uuidSetAdd(uuid_set, 11);
        uuidSetAdd(uuid_set, 13);
        uuidSetAdd(uuid_set, 20);
        uuidSetAdd(uuid_set, 19);
        uuidSetAdd(uuid_set, 1);
        uuidSetAdd(uuid_set, 12);
        uuidSetAdd(uuid_set, 3);
        uuidSetAdd(uuid_set, 13);
        uuidSetAdd(uuid_set, 13);
        uuidSetAdd(uuid_set, 14);
        uuidSetAdd(uuid_set, 12);

        test_cond("Manual created case: result should be 0-1,3,5-6,11-14,19-20",
            uuid_set->intervals->gno_start == 0 && uuid_set->intervals->gno_end == 1
            && uuid_set->intervals->next->gno_start == 3 && uuid_set->intervals->next->gno_end == 3
            && uuid_set->intervals->next->next->gno_start == 5 && uuid_set->intervals->next->next->gno_end == 6
            && uuid_set->intervals->next->next->next->gno_start == 11 && uuid_set->intervals->next->next->next->gno_end == 14
            && uuid_set->intervals->next->next->next->next->gno_start == 19 && uuid_set->intervals->next->next->next->next->gno_end == 20
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);
    }
    return 0;
}
#endif

#ifdef GTID_TEST_MAIN
int main(void) {
    return gtidTest();
}
#endif
