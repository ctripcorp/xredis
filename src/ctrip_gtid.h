#ifndef __REDIS_CTRIP_GTID_H
#define __REDIS_CTRIP_GTID_H

#include "sds.h"

typedef struct gtidInterval gtidInterval;
typedef struct uuidSet uuidSet;
typedef struct gtidSet gtidSet;

typedef long long int rpl_gno; // >= 1

struct gtidInterval {
    rpl_gno gno_start;
    rpl_gno gno_end;
    gtidInterval *next;
};

struct uuidSet {
    sds rpl_sid;
    gtidInterval* intervals;
    uuidSet *next;
};

struct gtidSet {
    uuidSet* uuid_sets;
};

/* uuid set implementation */
gtidInterval *gtidIntervalNew(rpl_gno);
gtidInterval *gtidIntervalNewRange(rpl_gno, rpl_gno);
uuidSet *uuidSetNew(const char*, rpl_gno);
uuidSet *uuidSetNewRange(const char*, rpl_gno, rpl_gno);
void uuidSetFree(uuidSet*);
sds uuidSetEncode(uuidSet*, sds);
int uuidSetAdd(uuidSet*, rpl_gno);
void uuidSetRaise(uuidSet*, rpl_gno);
int uuidSetContains(uuidSet*, rpl_gno);
rpl_gno uuidSetNext(uuidSet*);


/* gtid set implementation */
gtidSet *gtidSetNew();
void gtidSetFree(gtidSet*);
sds gtidSetEncode(gtidSet*, sds);
int gtidSetAdd(gtidSet*, const char*, rpl_gno);
void gtidSetRaise(gtidSet*, const char*, rpl_gno);

#endif  /* __REDIS_CTRIP_GTID_H */
