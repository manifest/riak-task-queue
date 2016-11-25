PROJECT = riaktq
PROJECT_DESCRIPTION = Riak Task Queue
PROJECT_VERSION = 0.1.0

DEPS = \
	riakc_pool \
	erlexec \
	jsx

dep_riakc_pool = git git://github.com/manifest/riak-connection-pool.git v0.2.0
dep_erlexec = git git://github.com/saleyn/erlexec.git 1.6.4
dep_jsx = git git://github.com/talentdeficit/jsx.git v2.8.0

SHELL_DEPS = tddreloader
SHELL_OPTS = \
	-eval 'application:ensure_all_started($(PROJECT), permanent)' \
	-s tddreloader start

include erlang.mk
