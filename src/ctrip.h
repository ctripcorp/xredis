/*
 * ctrip.h
 *
 *  Created on: Sep 21, 2017
 *      Author: mengwenchao
 */

#ifndef SRC_CTRIP_H_
#define SRC_CTRIP_H_

#define XREDIS_VERSION "2.1.0"
#define CONFIG_DEFAULT_SLAVE_REPLICATE_ALL 0

void xslaveofCommand(client *c);
void refullsyncCommand(client *c);

#ifdef REDIS_TEST
#define TEST(name) printf("test — %s\n", name);
#define TEST_DESC(name, ...) printf("test — " name "\n", __VA_ARGS__);
#define test_assert(e) do {							\
	if (!(e)) {				\
		printf(						\
		    "%s:%d: Failed assertion: \"%s\"\n",	\
		    __FILE__, __LINE__, #e);				\
		error++;						\
	}								\
} while (0)
#endif

#endif /* SRC_CTRIP_H_ */
