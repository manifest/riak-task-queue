PROJECT = riaktq
PROJECT_DESCRIPTION = Riak Task Queue

DEP_PLUGINS = \
	version.mk

DEPS = \
	riakc_pool

NO_AUTOPATCH = \
	riak_pb

dep_riakc_pool = git git://github.com/manifest/riak-connection-pool.git v0.2.1

BUILD_DEPS = version.mk
dep_version.mk = git git://github.com/manifest/version.mk.git master

TEST_DEPS = ct_helper
dep_ct_helper = git git://github.com/ninenines/ct_helper.git master

SHELL_DEPS = tddreloader
SHELL_OPTS = \
	-eval 'application:ensure_all_started($(PROJECT), permanent)' \
	-s tddreloader start

include erlang.mk

export DEVELOP_ENVIRONMENT = $(shell if [ -f .develop-environment ]; then cat .develop-environment; fi)
