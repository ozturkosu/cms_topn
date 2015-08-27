MODULE_big = cms_topn
OBJS =		\
			cms_topn.o \
			MurmurHash3.o \
			$(NULL)

EXTENSION = cms_topn
DATA =		\
			cms_topn--1.0.0.sql \
			$(NULL)


REGRESS = create add add_agg union union_agg results in_out_send_recv

EXTRA_CLEAN += -r $(RPM_BUILD_ROOT)

PG_CPPFLAGS += -fPIC
cms_topn.o: override CFLAGS += -std=c99
MurmurHash3.o: override CFLAGS += -std=c99

ifdef DEBUG
COPT		+= -O0
CXXFLAGS	+= -g -O0
endif

SHLIB_LINK	+= -lstdc++

ifndef PG_CONFIG
PG_CONFIG = pg_config
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
