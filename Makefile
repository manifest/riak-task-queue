PROJECT = riaktq
PROJECT_DESCRIPTION = Riak Task Queue
PROJECT_VERSION = 0.1.0

DEPS = \
	riakc_pool

dep_riakc_pool = git git://github.com/manifest/riak-connection-pool.git v0.2.0

SHELL_DEPS = tddreloader
SHELL_OPTS = \
	-eval 'application:ensure_all_started($(PROJECT), permanent)' \
	-s tddreloader start

include erlang.mk
