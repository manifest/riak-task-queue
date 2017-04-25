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

-module(riaktq_scheduler).
-behaviour(gen_statem).

-include_lib("riakc/include/riakc.hrl").
-include("riaktq_log.hrl").

%% API
-export([
	start_link/1
]).

%% GenStateM callbacks
-export([
	init/1,
	handle_event/4,
	callback_mode/0,
	terminate/3,
	code_change/4
]).

%% Definitions
-define(DEFAULT_SCHEDULE_INTERVAL, timer:minutes(1)).

%% Types
-record(proc, {
	pid :: pid(),
	ref :: reference()
}).

-type proc() :: #proc{}.

-record(state, {
	hproc  :: proc() | undefined,
	group  :: atom(),
	pool   :: riakc_pool:name(),
	bucket :: bucket_and_type(),
	index  :: binary(),
	tint   :: non_neg_integer(),
	tref   :: reference()
}).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link(map()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Conf) ->
	gen_statem:start_link({local, ?MODULE}, ?MODULE, Conf, []).

%% =============================================================================
%% GenStateM callbacks
%% =============================================================================

-spec init(map()) -> no_return().
init(Conf) ->
	Sdata =
		#state{
			pool = validate_riakc_pool(Conf),
			group = validate_group(Conf),
			bucket = validate_riak_bucket(Conf),
			index = validate_riak_index(Conf),
			tint = validate_schedule_interval(Conf, ?DEFAULT_SCHEDULE_INTERVAL),
			tref = erlang:start_timer(0, self(), try_schedule_tasks)},

	{ok, idle, Sdata}.

%% We aren't scheduling tasks at the moment (hproc=undefined): start do it and switch to the 'busy' state.
handle_event(internal, try_schedule_tasks, _Sname, #state{hproc=undefined, group=G, pool=P, bucket=B, index=I} =Sdata) ->
	{Hpid, Href} = riaktq_scheduler_proc:run(G, P, B, I),
	{next_state, busy, Sdata#state{hproc=#proc{pid=Hpid, ref=Href}}};
%% Time to obtain and schedule tasks: restart the timer and try to do it.
handle_event(info, {timeout, Tref0, Tmsg}, _Sname, #state{tint=Tint, tref=Tref0} =Sdata) ->
	Tref1 = handle_timer(Tref0, Tint, Tmsg),
	{keep_state, Sdata#state{tref=Tref1}, [{next_event, internal, Tmsg}]};
%% Scheduling process is succeed: switch to the 'idle' state.
handle_event(info, riaktq_schedule_done, busy, #state{hproc=#proc{ref=Href}} =Sdata) ->
	demonitor(Href, [flush]),
	{next_state, idle, Sdata#state{hproc=undefined}};
%% Scheduling process is failed (Reason=/=normal): switch to the 'idle' state.
handle_event(info, {'DOWN', Href, process, _Pid, Reason}, busy, #state{hproc=#proc{ref=Href}} =Sdata) when Reason =/= normal ->
	?ERROR_REPORT([{reason, scheduler_proc_exit}, {exception_reason, Reason}]),
	{next_state, idle, Sdata#state{hproc=undefined}};
%% Keep running
handle_event(_Mtype, _Mdata, _Sname, _Sdata) ->
	keep_state_and_data.

callback_mode() ->
	handle_event_function.

terminate(_Reason, _Sname, _Sdata) ->
	ok.

code_change(_VSN, State, Data, _Extra) ->
	{ok, State, Data}.

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec handle_timer(reference(), non_neg_integer(), any()) -> reference().
handle_timer(Ref, Interval, Message) ->
	_ = erlang:cancel_timer(Ref),
	erlang:start_timer(Interval, self(), Message).

-spec validate_group(map()) -> atom().
validate_group(#{group := Val}) when is_atom(Val) -> Val;
validate_group(#{group := Val})                   -> error({invalid_group, Val});
validate_group(_)                                 -> error(missing_group).

-spec validate_riakc_pool(map()) -> atom().
validate_riakc_pool(#{riak_connection_pool := Val}) when is_atom(Val) -> Val;
validate_riakc_pool(#{riak_connection_pool := Val})                   -> error({invalid_riak_connection_pool, Val});
validate_riakc_pool(_)                                                -> error(missing_riak_connection_pool).

-spec validate_schedule_interval(map(), non_neg_integer()) -> non_neg_integer().
validate_schedule_interval(#{schedule_interval := Val}, _) when is_integer(Val), Val > 0 -> Val;
validate_schedule_interval(#{schedule_interval := Val}, _)                               -> error({invalid_schedule_interval, Val});
validate_schedule_interval(_, Default)                                                   -> Default.

-spec validate_riak_bucket(map()) -> bucket_and_type().
validate_riak_bucket(#{riak_bucket := {Type,Name}=Val}) when is_binary(Type), is_binary(Name) -> Val;
validate_riak_bucket(#{riak_bucket := Val})                                                   -> error({invalid_riak_bucket, Val});
validate_riak_bucket(_)                                                                       -> error(missing_riak_bucket).

-spec validate_riak_index(map()) -> binary().
validate_riak_index(#{riak_index := Val}) when is_binary(Val) -> Val;
validate_riak_index(#{riak_index := Val})                     -> error({invalid_riak_index, Val});
validate_riak_index(_)                                        -> error(missing_riak_index).
