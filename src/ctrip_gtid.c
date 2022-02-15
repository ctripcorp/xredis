#include "server.h"
#include "sds.h"
#include "zmalloc.h"

gtidInterval *gtidIntervalNew(rpl_gno gno) {
    return gtidIntervalNewRange(gno, gno);
}

gtidInterval *gtidIntervalNewRange(rpl_gno start, rpl_gno end) {
    gtidInterval *interval = zmalloc(sizeof(*interval));
    interval->gno_start = start;
    interval->gno_end = end;
    interval->next = NULL;
    return interval;
}

uuidSet *uuidSetNew(const char* rpl_sid, rpl_gno gno) {
    return uuidSetNewRange(rpl_sid, gno, gno);
}

uuidSet *uuidSetNewRange(const char* rpl_sid, rpl_gno start, rpl_gno end) {
    uuidSet *uuid_set = zmalloc(sizeof(*uuid_set));
    uuid_set->rpl_sid = sdsnew(rpl_sid);
    uuid_set->intervals = gtidIntervalNewRange(start, end);
    uuid_set->next = NULL;
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

sds uuidSetEncode(uuidSet* uuid_set, sds src) {
    sdscat(src, uuid_set->rpl_sid);
    gtidInterval *cur = uuid_set->intervals;
    while(cur != NULL) {
        src = sdscat(src, ":");
        if (cur->gno_start == cur->gno_end) {
            src = sdscatfmt(src, "%I", cur->gno_start);
        } else {
            src = sdscatfmt(src, "%I-%I", cur->gno_start, cur->gno_end);
        }
        cur = cur->next;
    }
    return src;
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
        if (gno <= next->gno_end) {
            return 0;
        }
        cur = next;
        next = cur->next;
    }
    if (gno == cur->gno_end + 1) {
        cur->gno_end = gno;
        return 1;
    }
    if (gno > cur->gno_end + 1) {
        cur->next = gtidIntervalNew(gno);
        return 1;
    }
    return 0;
}

void uuidSetRaise(uuidSet *uuid_set, rpl_gno watermark) {
    gtidInterval *cur = uuid_set->intervals;
    if (watermark < cur->gno_start - 1) {
        uuid_set->intervals = gtidIntervalNewRange(1, watermark);
        uuid_set->intervals->next = cur;
        return;
    }

    while (cur != NULL) {
        if (watermark > cur->gno_end + 1) {
            gtidInterval *temp = cur;
            cur = cur->next;
            zfree(temp);
            continue;
        }

        if (watermark == cur->gno_end + 1) {
            if (cur->next == NULL) {
                cur->gno_start = 1;
                cur->gno_end = watermark;
                uuid_set->intervals = cur;
                break;
            }
            if (watermark == cur->next->gno_start - 1) {
                gtidInterval *prev = cur;
                cur = cur->next;
                zfree(prev);
                cur->gno_start = 1;
                break;
            } else {
                cur->gno_end = watermark;
                cur->gno_start = 1;
                break;
            }
        }
        if (watermark < cur->gno_start - 1) {
            gtidInterval *temp = cur;
            cur = gtidIntervalNewRange(1, watermark);
            cur->next = temp;
            break;
        } else {
            cur->gno_start = 1;
            break;
        }
    }
    if (cur == NULL) {
        uuid_set->intervals = gtidIntervalNewRange(1, watermark);
    } else {
        uuid_set->intervals = cur;
    }
}

gtidSet* gtidSetNew() {
    gtidSet *gtid_set = zmalloc(sizeof(*gtid_set));
    gtid_set->uuid_sets = NULL;
    return gtid_set;
}

void gtidSetFree(gtidSet* gtid_set) {
    uuidSet *next;
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        next = cur->next;
        uuidSetFree(cur);
        cur = next;
    }
    zfree(gtid_set);
}

sds gtidEncode(gtidSet *gtid_set, sds src) {
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        src = uuidSetEncode(cur, src);
        cur = cur->next;
        if (cur != NULL) {
            src = sdscat(src, ",");
        }
    }
    return src;
}

int gtidAdd(gtidSet *gtid_set, const char *rpl_sid, rpl_gno gno) {
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        if (strcmp(rpl_sid, cur->rpl_sid) == 0) {
            break;
        }
        cur = cur->next;
    }
    if (cur == NULL) {
        cur = uuidSetNew(rpl_sid, gno);
        cur->next = gtid_set->uuid_sets;
        gtid_set->uuid_sets = cur;
        return 1;
    } else {
        return uuidSetAdd(cur, gno);
    }
}

void gtidRaise(gtidSet *gtid_set, const char *rpl_sid, rpl_gno watermark) {
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        if (strcmp(rpl_sid, cur->rpl_sid) == 0) {
            break;
        }
        cur = cur->next;
    }
    if (cur == NULL) {
        cur = uuidSetNewRange(rpl_sid, 1, watermark);
        cur->next = gtid_set->uuid_sets;
        gtid_set->uuid_sets = cur;
    } else {
        uuidSetRaise(cur, watermark);
    }
}

#if defined(GTID_TEST_MAIN)
#include <stdio.h>
#include "testhelp.h"
#include "limits.h"

int gtidTest(void) {
    {
        /* gtid unit tests*/

        gtidSet *gtid_set = gtidSetNew();
        sds gtidset = sdsnew("");
        gtidset = gtidEncode(gtid_set, gtidset);

        test_cond("Create an empty gtid set",
            memcmp(gtidset, "\0", 1) == 0);

        sdsfree(gtidset);

        gtidAdd(gtid_set, "A", 1);
        gtidAdd(gtid_set, "A", 2);
        gtidAdd(gtid_set, "B", 3);

        test_cond("Add A:1 A:2 B:3 to empty gtid set",
            memcmp(gtid_set->uuid_sets->rpl_sid, "B\0", 1) == 0
            && gtid_set->uuid_sets->intervals->gno_start == 3 && gtid_set->uuid_sets->intervals->gno_end == 3
            && memcmp(gtid_set->uuid_sets->next->rpl_sid, "A\0", 1) == 0
            && gtid_set->uuid_sets->next->intervals->gno_start == 1 && gtid_set->uuid_sets->next->intervals->gno_end == 2);

        gtidset = sdsnew("");
        gtidset = gtidEncode(gtid_set, gtidset);

        test_cond("Add A:1 A:2 B:3 to empty gtid set (encode)",
            strcmp(gtidset, "B:3,A:1-2") == 0);

        sdsfree(gtidset);

        gtidAdd(gtid_set, "B", 7);
        gtidRaise(gtid_set, "A", 5);
        gtidRaise(gtid_set, "B", 5);
        gtidRaise(gtid_set, "C", 10);

        gtidset = sdsnew("");
        gtidset = gtidEncode(gtid_set, gtidset);

        test_cond("Raise A & B to 5, C to 10, towards C:1-10,B:3:7,A:1-2",
            strcmp(gtidset, "C:1-10,B:1-5:7,A:1-5") == 0);

        sdsfree(gtidset);

        gtidSetFree(gtid_set);

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

        uuid_set = uuidSetNew("A", 1);
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

        test_cond("Manual created case: result should be A:1:3:5-6:11-14:19-20",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 1
            && uuid_set->intervals->next->gno_start == 3 && uuid_set->intervals->next->gno_end == 3
            && uuid_set->intervals->next->next->gno_start == 5 && uuid_set->intervals->next->next->gno_end == 6
            && uuid_set->intervals->next->next->next->gno_start == 11 && uuid_set->intervals->next->next->next->gno_end == 14
            && uuid_set->intervals->next->next->next->next->gno_start == 19 && uuid_set->intervals->next->next->next->next->gno_end == 20
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        sds uuidset = sdsnew("");
        uuidset = uuidSetEncode(uuid_set, uuidset);

        test_cond("Manual created case (encode): result should be A:1:3:5-6:1-14:19-20",
            strcmp(uuidset, "A:1:3:5-6:11-14:19-20") == 0);

        sdsfree(uuidset);

        uuidSetRaise(uuid_set, 30);
        test_cond("Manual created case (raise to 30)",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 30
            && uuid_set->intervals->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);


        uuidSetFree(uuid_set);

        uuid_set = uuidSetNew("A", 5);
        uuidSetAdd(uuid_set, 6);
        uuidSetAdd(uuid_set, 8);
        uuidSetAdd(uuid_set, 9);

        test_cond("add 9 to 5-6,8-9",
            uuidSetAdd(uuid_set, 9) == 0);

        uuidSetFree(uuid_set);

        uuid_set = uuidSetNew("A", 5);
        uuidSetAdd(uuid_set, 6);

        test_cond("add 6 to 5-6",
            uuidSetAdd(uuid_set, 6) == 0);

        uuidSetFree(uuid_set);

        uuid_set = uuidSetNew("A", 5);
        uuidSetRaise(uuid_set, 3);

        test_cond("raise to 3 towards A:5",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 3
            && uuid_set->intervals->next->gno_start == 5 && uuid_set->intervals->next->gno_end == 5
            && uuid_set->intervals->next->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);


        uuid_set = uuidSetNew("A", 5);
        uuidSetRaise(uuid_set, 4);

        test_cond("raise to 4 towards A:5",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 5
            && uuid_set->intervals->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);


        uuid_set = uuidSetNew("A", 5);
        uuidSetRaise(uuid_set, 6);

        test_cond("raise to 6 towards A:5",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 6
            && uuid_set->intervals->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);


        uuid_set = uuidSetNew("A", 5);
        uuidSetAdd(uuid_set, 7);
        uuidSetRaise(uuid_set, 6);

        test_cond("raise to 6 towards A:5:7",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 7
            && uuid_set->intervals->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);


        uuid_set = uuidSetNew("A", 5);
        uuidSetAdd(uuid_set, 8);
        uuidSetRaise(uuid_set, 6);

        test_cond("raise to 6 towards A:5:8",
            uuid_set->intervals->gno_start == 1 && uuid_set->intervals->gno_end == 6
            && uuid_set->intervals->next->gno_start == 8 && uuid_set->intervals->next->gno_end == 8
            && uuid_set->intervals->next->next == NULL
            && memcmp(uuid_set->rpl_sid, "A\0", 2) == 0);

        uuidSetFree(uuid_set);


        /* interval unit tests*/

        gtidInterval *interval = gtidIntervalNew(9);
        test_cond("Create an new gtid interval with 9",
            interval->gno_start == 9 && interval->gno_end == 9);

        zfree(interval);
    }
    test_report()
    return 0;
}
#endif

#ifdef GTID_TEST_MAIN
int main(void) {
    return gtidTest();
}
#endif
