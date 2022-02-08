#include "server.h"

char* gtidEncode(gtidSet *gtid_set) {

    return "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA:1-7";
}

gtidSet* gtidDecode(char* gtid) {

    return NULL;
}

gtidSet* gtidAdd(gtidSet *gtid_set, char *gtid) {

    return NULL;
}

gtidSet* gtidRaise(gtidSet *gtid_set, char *rpl_sid, rpl_gno watermark) {

    return NULL;
}