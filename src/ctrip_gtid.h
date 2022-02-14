#ifndef __REDIS_CTRIP_GTID_H
#define __REDIS_CTRIP_GTID_H

typedef struct gtidInterval gtidInterval;
typedef struct uuidSet uuidSet;
typedef struct gtidSet gtidSet;

typedef long long int rpl_gno;

struct gtidInterval {

    rpl_gno gno_start;

    rpl_gno gno_end;
};

struct uuidSet {

    char* rpl_sid;

    gtidInterval* gtid_intervals;
};

struct gtidSet {

    uuidSet* uuid_sets;
};

gtidInterval *gtidIntervalNew(rpl_gno);

int gtidIntervalAdd(gtidInterval*, rpl_gno);

uuidSet *uuidSetNew(const char*, rpl_gno);

gtidSet *gtidSetNew();

sds gtidEncode(gtidSet*);

gtidSet *gtidAdd(gtidSet*, const char*);

gtidSet *gtidRaise(gtidSet*, const char*, rpl_gno);

#endif  /* __REDIS_CTRIP_GTID_H */
