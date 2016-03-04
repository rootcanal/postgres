# contrib/gapless_seq/Makefile

MODULE_big = gapless_seq
OBJS = gapless_seq.o
PG_CPPFLAGS = -I$(libpq_srcdir)

EXTENSION = gapless_seq
DATA = gapless_seq--1.0.sql

EXTRA_CLEAN = $(pg_regress_clean_files) ./regression_output ./isolation_output

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gapless_seq
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

check: regresscheck isolationcheck

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress all

submake-isolation:
	$(MAKE) -C $(top_builddir)/src/test/isolation all

submake-gapless_seq:
	$(MAKE) -C $(top_builddir)/contrib/gapless_seq

REGRESSCHECKS=gapless_seq

regresscheck: all | submake-regress submake-gapless_seq temp-install
	$(MKDIR_P) regression_output
	$(pg_regress_check) \
	    --temp-instance=./tmp_check \
	    --outputdir=./regression_output \
	    $(REGRESSCHECKS)

regresscheck-install-force: | submake-regress submake-gapless_seq temp-install
	$(pg_regress_installcheck) \
	    $(REGRESSCHECKS)

ISOLATIONCHECKS=concurrency

isolationcheck: all | submake-isolation submake-gapless_seq temp-install
	$(MKDIR_P) isolation_output
	$(pg_isolation_regress_check) \
	    --outputdir=./isolation_output \
	    $(ISOLATIONCHECKS)

isolationcheck-install-force: all | submake-isolation submake-gapless_seq temp-install
	$(pg_isolation_regress_installcheck) \
	    $(ISOLATIONCHECKS)

PHONY: submake-gapless_seq submake-regress check \
	regresscheck regresscheck-install-force \
	isolationcheck isolationcheck-install-force

temp-install: EXTRA_INSTALL=contrib/gapless_seq
