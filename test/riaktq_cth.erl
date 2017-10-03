%% ----------------------------------------------------------------------------
%% The MIT License
%%
%% Copyright (c) 2016-2017 Andrei Nesterov <ae.nesterov@gmail.com>
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to
%% deal in the Software without restriction, including without limitation the
%% rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
%% sell copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
%% IN THE SOFTWARE.
%% ----------------------------------------------------------------------------

-module(riaktq_cth).

-include_lib("riakc/include/riakc.hrl").

%% API
-export([
	init_config/0,
	task_open/3,
	task_remove/2,
	task_wait/2,
	scheduler_wait/1,
	make_taskname/0
]).

%% =============================================================================
%% API
%% =============================================================================

-spec init_config() -> list().
init_config() ->
	Config =
		try
			{ok, S, _} = erl_scan:string(os:getenv("DEVELOP_ENVIRONMENT")),
			{ok, Conf} = erl_parse:parse_term(S),
			maps:fold(fun(Key, Val, Acc) -> [{Key, Val}|Acc] end, [], Conf)
		catch _:Reason -> error({missing_develop_environment, ?FUNCTION_NAME, Reason}) end,

	{_, #{host := Host, port := Port}} = lists:keyfind(kv_protobuf, 1, Config),
	KVpool = kv_protobuf,
	Bucket = {<<"riaktq_task_t">>, <<"task">>},
	Index = <<"riaktq_task_idx">>,
	Interval = timer:seconds(2),
	init_riaktq_application(KVpool, Bucket, Index, Interval, Host, Port),

	[{bucket, Bucket}, {index, Index}, {pool, KVpool}, {interval, Interval} | Config].

-spec task_open(binary(), riaktq_riakc:task(), list()) -> pid().
task_open(Tid, Tobj, Config) ->
	{_, Bucket} = lists:keyfind(bucket, 1, Config),
	{_, Pool} = lists:keyfind(pool, 1, Config),
	Pid = riakc_pool:lock(Pool),
	riaktq_task:open(Pid, Bucket, Tid, Tobj),
	riakc_pool:unlock(Pool, Pid).

-spec task_remove(binary(), list()) -> ok.
task_remove(Tid, Config) ->
	{_, Bucket} = lists:keyfind(bucket, 1, Config),
	{_, Pool} = lists:keyfind(pool, 1, Config),
	Pid = riakc_pool:lock(Pool),
	riakc_pb_socket:delete(Pid, Bucket, Tid),
	riakc_pool:unlock(Pool, Pid).

-spec task_wait(binary(), list()) -> any().
task_wait(Tid, Config) ->
	{_, Bucket} = lists:keyfind(bucket, 1, Config),
	{_, Pool} = lists:keyfind(pool, 1, Config),
	Pid = riakc_pool:lock(Pool),
	Tobj = wait(Pid, Bucket, Tid, 10, 100),
	riakc_pool:unlock(Pool, Pid),
	Tobj.

%% We wait at least 2 scheduler intervals: assign + commit.
-spec scheduler_wait(list()) -> ok.
scheduler_wait(Config) ->
	{_, Internal} = lists:keyfind(interval, 1, Config),
	timer:sleep(Internal *2).

-spec make_taskname() -> binary().
make_taskname() ->
	list_to_binary(vector(128, alphanum_chars())).

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec wait(pid(), binary(), binary(), non_neg_integer(), non_neg_integer()) -> any().
wait(Pid, Bucket, Tid, N, Timeout) ->
	{ok, Task} = riakc_pb_socket:fetch_type(Pid, Bucket, Tid),
	case riakc_map:find({<<"out">>, register}, Task) of
		{ok, _Obj} -> Task;
		_          -> timer:sleep(Timeout), wait(Pid, Bucket, Tid, N -1, Timeout)
	end.

-spec init_riaktq_application(atom(), binary(), binary(), non_neg_integer(), binary(), non_neg_integer()) -> ok.
init_riaktq_application(KVpool, Bucket, Index, Interval, Host, Port) ->
	RiakPoolConf =
		#{name => KVpool,
			size => 5,
			connection =>
				#{host => Host,
					port => Port,
					options => [queue_if_disconnected]}},
	supervisor:start_child(whereis(riaktq_sup), riakc_pool:child_spec(RiakPoolConf)),

	%% Set scheduler's node name (we use same node for the sheduler and instances)
	SchedulerNode = node(),

	%% Creating a scheduler and adding it to the supervision tree.
	Group = riaktq_instance_sup,
	SchedulerConf =
		#{group => Group,
			riak_connection_pool => KVpool,
			riak_bucket => Bucket,
			riak_index => Index,
			interval => Interval},
	supervisor:start_child(whereis(riaktq_sup), riaktq:scheduler_spec({Group, scheduler}, SchedulerConf)),

	%% Creating five instances with `riaktq_echo` handler,
	%% and adding them to the supervision tree.
	[begin
		Name = <<"echo-", (integer_to_binary(N))/binary>>,
		InstanceConf =
			#{group => Group, 
				name => Name,
				module => riaktq_echo,
				options => #{},
				transport_options => #{scheduler_node => SchedulerNode}},
		supervisor:start_child(whereis(Group), riaktq:instance_spec(Name, InstanceConf))
	end || N <- lists:seq(1, 5)],

	ok.

-spec oneof(list()) -> integer().
oneof(L) ->
	lists:nth(rand:uniform(length(L)), L).

-spec vector(non_neg_integer(), list()) -> list().
vector(MaxSize, L) ->
	vector(0, MaxSize, L, []).

-spec vector(non_neg_integer(), non_neg_integer(), list(), list()) -> list().
vector(Size, MaxSize, L, Acc) when Size < MaxSize ->
	vector(Size +1, MaxSize, L, [oneof(L)|Acc]);
vector(_, _, _, Acc) ->
	Acc.

-spec alphanum_chars() -> list().
alphanum_chars() ->
	"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".
