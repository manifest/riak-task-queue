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

-module(riaktq_scheduler_proc).

-include_lib("riakc/include/riakc.hrl").
-include("riaktq_log.hrl").

%% API
-export([
	run/5
]).

%% Internal callbacks
-export([
	loop/6
]).

%% =============================================================================
%% API
%% =============================================================================

-spec run(atom(), riakc_pool:name(), bucket_and_type(), binary(), atom()) -> {pid(), reference()}.
run(Group, Pool, Bucket, Index, EvM) ->
	spawn_monitor(?MODULE, loop, [self(), Group, Pool, Bucket, Index, EvM]).

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec loop(pid(), atom(), riakc_pool:name(), bucket_and_type(), binary(), atom()) -> no_return().
loop(Parent, Group, Pool, Bucket, Index, EvM) ->
	_ = handle(Group, Pool, Bucket, Index, EvM),
	Parent ! riaktq_schedule_done.

-spec handle(atom(), riakc_pool:name(), bucket_and_type(), binary(), atom()) -> any().
handle(Group, Pool, Bucket, Index, EvM) ->
	KVpid = riakc_pool:lock(Pool),
	{NextUpInstances, TasksToCommit, TasksOnInstances} =
		handle_instance_output(supervisor:which_children(Group), KVpid, Bucket, EvM),

	handle_commit(TasksToCommit),
	handle_assign(NextUpInstances, todo_tasks(KVpid, Index, Bucket), KVpid, Bucket, EvM),
	handle_rollback(TasksOnInstances, gb_sets:from_list(assigned_tasks(KVpid, Index)), KVpid, Bucket, EvM),
	riakc_pool:unlock(Pool, KVpid).

handle_instance_output(Children, KVpid, Bucket, EvM) ->
	handle_instance_output(Children, KVpid, Bucket, EvM, [], [], gb_sets:new()).

handle_instance_output([{Name, Pid, _, _}|T], KVpid, Bucket, EvM, NextUpInstances, TasksToCommit, TasksOnInstances) ->
	AddList = fun(L, Acc) -> lists:foldl(fun gb_sets:add_element/2, Acc, L) end,
	try riaktq_instance:get(Pid) of
		#{input := In, output := Out, status := <<"idle">>} ->
			handle_instance_output(T, KVpid, Bucket, EvM,
				[{Pid, Name}|NextUpInstances],
				[{Pid, handle_close(Out, KVpid, Bucket, EvM)}|TasksToCommit],
				AddList(In, AddList(Out, TasksOnInstances)));
		#{input := In, output := Out} ->
			handle_instance_output(T, KVpid, Bucket, EvM,
				NextUpInstances,
				[{Pid, handle_close(Out, KVpid, Bucket, EvM)}|TasksToCommit],
				AddList(In, AddList(Out, TasksOnInstances)))
	catch T:R ->
		?ERROR_REPORT([{instance, Name}], T, R),
		handle_instance_output(T, KVpid, Bucket, EvM, NextUpInstances, TasksToCommit, TasksOnInstances)
	end;
handle_instance_output([], _KVpid, _Bucket, _EvM, NextUpInstances, TasksToCommit, TasksOnInstances) ->
	{NextUpInstances, TasksToCommit, TasksOnInstances}.

-spec handle_close([map()], pid(), bucket_and_type(), atom()) -> [binary()].
handle_close(Out, KVpid, Bucket, EvM) ->
	handle_close(Out, KVpid, Bucket, EvM, []).

-spec handle_close([map()], pid(), bucket_and_type(), atom(), [binary()]) -> [binary()].
handle_close([M|T], KVpid, Bucket, EvM, Acc) ->
	#{id := Id,
		status := Status,
		out := Out,
		sat := CreatedAt,
		laf := LastedFor} = M,

	case maybe_report(EvM, close, Bucket, Id, riaktq_task:close(KVpid, Bucket, Id, Status, [return_body], fun(T0) ->
		T1 = riakc_map:update({<<"out">>, register}, fun(Obj) -> riakc_register:set(Out, Obj) end, T0),
		T2 = riakc_map:update({<<"sat">>, register}, fun(Obj) -> riakc_register:set(integer_to_binary(CreatedAt), Obj) end, T1),
		T3 = riakc_map:update({<<"laf">>, register}, fun(Obj) -> riakc_register:set(integer_to_binary(LastedFor), Obj) end, T2),
		T3
	end)) of
		%% Status of the task could be changed after execution.
		%% So that we commit all the tasks (to clean up the instance).
		%% Such tasks are already completed or will be processed on the next iteration.
		_ -> handle_close(T, KVpid, Bucket, EvM, [Id|Acc])
	end;
handle_close([], _KVpid, _Bucket, _EvM, Acc) ->
	Acc.

-spec handle_commit([{pid(), [binary()]}]) -> ok.
handle_commit(TasksToCommit) ->
	[riaktq_instance:commit(Pid, Lout) || {Pid, Lout} <- TasksToCommit],
	ok.

-spec handle_assign([{pid(), binary()}], [binary()], pid(), bucket_and_type(), atom()) -> ok.
handle_assign([{Pid, Assignee}|Tinst]=NextUpInstances, [Id|Ttask], KVpid, Bucket, EvM) ->
	case maybe_report(EvM, assign, Bucket, Id, riaktq_task:assign(KVpid, Bucket, Id, Assignee, [return_body], fun(Obj) -> Obj end)) of
		{ok, T} ->
			In = riakc_map:fetch({<<"in">>, register}, T),
			Tags = case riakc_map:find({<<"tags">>, set}, T) of {ok, Val} -> Val; _ -> [] end,
			%% Dispatching the task to the instance
			riaktq_instance:put(Pid, [#{id => Id, in => In, tags => Tags}]),
			%% Move to a next instance and a next task
			handle_assign(Tinst, Ttask, KVpid, Bucket, EvM);
		_ ->
			%% Try to dispatch a next message to the instance
			handle_assign(NextUpInstances, Ttask, KVpid, Bucket, EvM)
	end;
handle_assign(_NextUpInstances, [], _KVpid, _Bucket, _EvM) -> ok;
handle_assign([], _TasksToDo, _KVpid, _Bucket, _EvM)       -> ok.

-spec handle_rollback(Ids, Ids, pid(), bucket_and_type(), atom()) -> ok when Ids :: gb_sets:set(binary()).
handle_rollback(TasksOnInstances, AssignedTasks, KVpid, Bucket, EvM) ->
	handle_rollback(gb_sets:to_list(gb_sets:subtract(AssignedTasks, TasksOnInstances)), KVpid, Bucket, EvM).

-spec handle_rollback([binary()], pid(), bucket_and_type(), atom()) -> ok.
handle_rollback([Id|T], KVpid, Bucket, EvM) ->
	%% Rollback a state of the lost task
	_ = maybe_report(EvM, rollback, Bucket, Id, riaktq_task:rollback(KVpid, Bucket, Id, [return_body], fun(Obj) -> Obj end)),
	handle_rollback(T, KVpid, Bucket, EvM);
handle_rollback([], _KVpid, _Bucket, _EvM) ->
	ok.

-spec todo_tasks(pid(), binary(), bucket_and_type()) -> [binary()].
todo_tasks(KVpid, Index, Bucket) ->
	{_BucketType, BucketName} = Bucket,
	riaktq_task:list(
		KVpid,
		Index,
		#{fq => <<"_yz_rb:", BucketName/binary, " AND status_register:todo">>,
			sort => <<"priority_register asc, cat_register asc">>}).

-spec assigned_tasks(pid(), binary()) -> [binary()].
assigned_tasks(KVpid, Index) ->
	riaktq_task:list(KVpid, Index, #{fq => <<"status_register:nextup AND assignee:*">>}).

-spec maybe_report(atom(), atom(), bucket_and_type(), binary(), Result) -> Result when Result :: riaktq_task:transition_result().
maybe_report(undefined, _Transition, _Bucket, _Id, Result) -> Result;
maybe_report(EvM, Transition, Bucket, Id, Result) ->
	try
		case Result of
			{ok, Task}      -> riaktq_eventm_task:report_transition_data(EvM, Transition, Bucket, Id, Task);
			{error, Reason} -> riaktq_eventm_task:report_transition_error(EvM, Transition, Bucket, Id, Reason)
		end
	catch T:R -> ?ERROR_REPORT([{event_manager, EvM}], T, R) end,
	Result.
