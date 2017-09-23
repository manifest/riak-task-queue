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
%% INOutput THE SOFTWARE.
%% ----------------------------------------------------------------------------

-module(riaktq_observer_proc).

-include("riaktq_log.hrl").

%% API
-export([
	run/4
]).

%% Internal callbacks
-export([
	loop/5
]).

%% =============================================================================
%% API
%% =============================================================================

-spec run(riakc_pool:name(), binary(), riaktq_observer:query(), atom()) -> {pid(), reference()}.
run(Pool, Index, Queries, EvM) ->
	spawn_monitor(?MODULE, loop, [self(), Pool, Index, Queries, EvM]).

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec loop(pid(), riakc_pool:name(), binary(), riaktq_observer:query(), atom()) -> no_return().
loop(Parent, Pool, Index, Queries, EvM) ->
	_ = handle(Pool, Index, Queries, EvM),
	Parent ! riaktq_observe_done.

-spec handle(riakc_pool:name(), binary(), riaktq_observer:query(), atom()) -> any().
handle(Pool, Index, Queries, EvM) ->
	KVpid = riakc_pool:lock(Pool),
	lists:foreach(
		fun(Query) ->
			try execute_query(KVpid, Index, Query) of
				Result  -> maybe_report(EvM, Query, Result)
			catch T:R -> ?ERROR_REPORT([{observe_query, Query}], T, R)
			end
		end, Queries),
	riakc_pool:unlock(Pool, KVpid).

-spec execute_query(pid(), binary(), riaktq_observer:query()) -> [binary()].
execute_query(KVpid, Index, Query) ->
	Status = maps:get(status, Query, <<$*>>),
	AgeUs = maps:get(age, Query, 0) *1000000,
	NowUs = riaktq:unix_time_us(),
	ToUs = integer_to_binary(NowUs -AgeUs),
	riaktq_task:list(
		KVpid,
		Index,
		#{fq => <<"cat_register:[* TO ", ToUs/binary, "] AND status_register:", Status/binary>>}).

-spec maybe_report(atom(), riaktq_observer:query(), Result) -> Result when Result :: [binary()].
maybe_report(undefined, _Query, Result)     -> Result;
maybe_report(_EvM, _Query, [] =Result)      -> Result;
maybe_report(EvM, #{id := QueryId}, Result) ->
	try riaktq_eventm_query:report_query_result(EvM, QueryId, Result)
	catch T:R -> ?ERROR_REPORT([{event_manager, EvM}], T, R) end,
	Result.
