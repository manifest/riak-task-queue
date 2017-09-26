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

-module(riaktq_observer).
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
-define(DEFAULT_OBSERVE_TIME, {0,0,0}).
-define(DEFAULT_OBSERVE_INTERVAL, timer:minutes(1)).

%% Types
-record(proc, {
	pid :: pid(),
	ref :: reference()
}).

-type proc() :: #proc{}.
-type query() :: #{id => any(), age := non_neg_integer(), status := binary()}.

-record(state, {
	hproc   :: proc() | undefined,
	pool    :: riakc_pool:name(),
	index   :: binary(),
	eventm  :: atom() | undefined,
	queries :: [query()],
	time    :: calendar:time(),
	ldate   :: calendar:date() | undefined,
	tint    :: non_neg_integer(),
	tref    :: reference()
}).

-export_type([query/0]).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link(map()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Conf) ->
	gen_statem:start_link(?MODULE, Conf, []).

%% =============================================================================
%% GenStateM callbacks
%% =============================================================================

-spec init(map()) -> no_return().
init(Conf) ->
	Sdata =
		#state{
			pool = validate_riakc_pool(Conf),
			index = validate_riak_index(Conf),
			eventm = validate_event_manager(Conf, undefined),
			queries = validate_queries(Conf, []),
			time = validate_time(Conf, ?DEFAULT_OBSERVE_TIME),
			tint = validate_interval(Conf, ?DEFAULT_OBSERVE_INTERVAL),
			tref = erlang:start_timer(0, self(), try_observe_tasks)},

	{ok, idle, Sdata}.

%% We aren't observing tasks at the moment (hproc=undefined): start do it and switch to the 'busy' state.
handle_event(internal, try_observe_tasks, _Sname, #state{hproc=undefined, ldate=Ldate, time=Time, queries=Qs, pool=P, index=I, eventm=EvM} =Sdata) ->
	{DateNow, TimeNow} = calendar:universal_time(),
	DoNothing = fun() -> {next_state, idle, Sdata} end,
	MaybeObserve =
		fun() ->
			case calendar:time_to_seconds(TimeNow) > calendar:time_to_seconds(Time) of
				true ->
					%% Lets observe
					{Hpid, Href} = riaktq_observer_proc:run(P, I, Qs, EvM),
					{next_state, busy, Sdata#state{hproc=#proc{pid=Hpid, ref=Href}, ldate=DateNow}};
				_ ->
					%% It's not a right time
					DoNothing()
			end
		end,
	case Ldate of
		undefined -> MaybeObserve();
		DateNow   -> DoNothing();
		_         -> MaybeObserve()
	end;
%% Time to observe tasks: restart the timer and try to do it.
handle_event(info, {timeout, Tref0, Tmsg}, _Sname, #state{tint=Tint, tref=Tref0} =Sdata) ->
	Tref1 = handle_timer(Tref0, Tint, Tmsg),
	{keep_state, Sdata#state{tref=Tref1}, [{next_event, internal, Tmsg}]};
%% Scheduling process is succeed: switch to the 'idle' state.
handle_event(info, riaktq_observe_done, busy, #state{hproc=#proc{ref=Href}} =Sdata) ->
	demonitor(Href, [flush]),
	{next_state, idle, Sdata#state{hproc=undefined}};
%% Scheduling process is failed (Reason=/=normal): switch to the 'idle' state.
handle_event(info, {'DOWN', Href, process, _Pid, Reason}, busy, #state{hproc=#proc{ref=Href}} =Sdata) when Reason =/= normal ->
	?ERROR_REPORT([{reason, observer_proc_exit}, {exception_reason, Reason}]),
	{next_state, idle, Sdata#state{hproc=undefined, ldate=undefined}};
%% Keep running
handle_event(_Mtype, _Mdata, _Sname, _Sdata) ->
	error_logger:info_report([?FUNCTION_NAME, timeout, _Mdata, _Sname, _Sdata]),
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

-spec validate_riakc_pool(map()) -> atom().
validate_riakc_pool(#{riak_connection_pool := Val}) when is_atom(Val) -> Val;
validate_riakc_pool(#{riak_connection_pool := Val})                   -> error({invalid_riak_connection_pool, Val});
validate_riakc_pool(_)                                                -> error(missing_riak_connection_pool).

-spec validate_queries(map(), [query()]) -> [query()].
validate_queries(#{queries := Val}, _) ->
	_ =
		try
			lists:foreach(
				fun(Q) ->
					_ = maps:get(key, Q),
					_ = case maps:find(age, Q) of {ok, Age} when is_integer(Age), Age >= 0 -> ok; error -> ok end,
					_ = case maps:find(status, Q) of {ok, Status} when is_binary(Status) -> ok; error -> ok end
				end, Val)
		catch _:_ -> error({invalid_query, Val}) end,
	Val;
validate_queries(_, Default) -> Default.

-spec validate_time(map(), Time) -> Time when Time :: {0..23, 0..59, 0..59}.
validate_time(#{time := {H, M, S} = Val}, _) when is_integer(H), H >= 0, H =< 23, is_integer(M), M >= 0, M =< 59, is_integer(S), M >= 0, M =< 59 -> Val;
validate_time(#{time := Val}, _)                                                                                                                 -> error({invalid_time, Val});
validate_time(_, Default)                                                                                                                                -> Default.

-spec validate_interval(map(), non_neg_integer()) -> non_neg_integer().
validate_interval(#{interval := Val}, _) when is_integer(Val), Val > 0 -> Val;
validate_interval(#{interval := Val}, _)                               -> error({invalid_interval, Val});
validate_interval(_, Default)                                                   -> Default.

-spec validate_event_manager(map(), atom()) -> atom().
validate_event_manager(#{event_manager := Val}, _) when is_atom(Val) -> Val;
validate_event_manager(#{event_manager := Val}, _)                   -> error({invalid_event_manager, Val});
validate_event_manager(_, Default)                                   -> Default.

-spec validate_riak_index(map()) -> binary().
validate_riak_index(#{riak_index := Val}) when is_binary(Val) -> Val;
validate_riak_index(#{riak_index := Val})                     -> error({invalid_riak_index, Val});
validate_riak_index(_)                                        -> error(missing_riak_index).
