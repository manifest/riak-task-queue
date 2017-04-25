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
	run/4
]).

%% Internal callbacks
-export([
	loop/5
]).

%% =============================================================================
%% API
%% =============================================================================

-spec run(atom(), riakc_pool:name(), bucket_and_type(), binary()) -> {pid(), reference()}.
run(Group, Pool, Bucket, Index) ->
	spawn_monitor(?MODULE, loop, [self(), Group, Pool, Bucket, Index]).

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec loop(pid(), atom(), riakc_pool:name(), bucket_and_type(), binary()) -> no_return().
loop(Parent, Group, Pool, Bucket, Index) ->
	_ = handle(Group, Pool, Bucket, Index),
	Parent ! riaktq_schedule_done.

-spec handle(atom(), riakc_pool:name(), bucket_and_type(), binary()) -> any().
handle(Group, Pool, Bucket, Index) ->
	RiakPid = riakc_pool:lock(Pool),
	{NextUpInstances, TasksToCommit, TasksOnInstances} =
		handle_instance_output(supervisor:which_children(Group), RiakPid, Bucket),

	handle_commit(TasksToCommit),
	handle_assign(NextUpInstances, todo_tasks(RiakPid, Index), RiakPid, Bucket),
	handle_rollback(TasksOnInstances, gb_sets:from_list(assigned_tasks(RiakPid, Index)), RiakPid, Bucket),
	riakc_pool:unlock(Pool, RiakPid).

handle_instance_output(Children, RiakPid, Bucket) ->
	handle_instance_output(Children, RiakPid, Bucket, [], [], gb_sets:new()).

handle_instance_output([{Name, Pid, _, _}|T], RiakPid, Bucket, NextUpInstances, TasksToCommit, TasksOnInstances) ->
	AddList = fun(L, Acc) -> lists:foldl(fun gb_sets:add_element/2, Acc, L) end,
	try riaktq_instance:get(Pid) of
		#{input := In, output := Out, status := <<"idle">>} ->
			handle_instance_output(T, RiakPid, Bucket,
				[{Pid, Name}|NextUpInstances],
				[{Pid, handle_close(Out, RiakPid, Bucket)}|TasksToCommit],
				AddList(In, AddList(Out, TasksOnInstances)));
		#{input := In, output := Out} ->
			handle_instance_output(T, RiakPid, Bucket,
				NextUpInstances,
				[{Pid, handle_close(Out, RiakPid, Bucket)}|TasksToCommit],
				AddList(In, AddList(Out, TasksOnInstances)))
	catch T:R ->
		?ERROR_REPORT([{reason, bad_output}, {instance, Name}], T, R),
		handle_instance_output(T, RiakPid, Bucket, NextUpInstances, TasksToCommit, TasksOnInstances)
	end;
handle_instance_output([], _RiakPid, _Bucket, NextUpInstances, TasksToCommit, TasksOnInstances) ->
	{NextUpInstances, TasksToCommit, TasksOnInstances}.

-spec handle_close([map()], pid(), bucket_and_type()) -> [binary()].
handle_close(Out, RiakPid, Bucket) ->
	handle_close(Out, RiakPid, Bucket, []).

-spec handle_close([map()], pid(), bucket_and_type(), [binary()]) -> [binary()].
handle_close([M|T], RiakPid, Bucket, Acc) ->
	#{id := Id,
		status := Status,
		out := Out,
		sat := CreatedAt,
		laf := LastedFor} = M,

	case riaktq_task:close(RiakPid, Bucket, Id, Status, fun(T0) ->
		T1 = riakc_map:update({<<"out">>, register}, fun(Obj) -> riakc_register:set(Out, Obj) end, T0),
		T2 = riakc_map:update({<<"sat">>, register}, fun(Obj) -> riakc_register:set(integer_to_binary(CreatedAt), Obj) end, T1),
		T3 = riakc_map:update({<<"laf">>, register}, fun(Obj) -> riakc_register:set(integer_to_binary(LastedFor), Obj) end, T2),
		T3
	end) of
		{ok, _}         -> handle_close(T, RiakPid, Bucket, [Id|Acc]);
		{error, Reason} ->
			%% Status of the task has been changed after execution.
			%% We commit the task (to clean up the instance), but keep task's status.
			?WARNING_REPORT([{reason, bad_status}, {exception_reason, Reason}]),
			handle_close(T, RiakPid, Bucket, [Id|Acc])
	end;
handle_close([], _RiakPid, _Bucket, Acc) ->
	Acc.

-spec handle_commit([{pid(), [binary()]}]) -> ok.
handle_commit(TasksToCommit) ->
	[riaktq_instance:commit(Pid, Lout) || {Pid, Lout} <- TasksToCommit],
	ok.

-spec handle_assign([{pid(), binary()}], [binary()], pid(), bucket_and_type()) -> ok.
handle_assign([{Pid, Assignee}|Tinst]=NextUpInstances, [Id|Ttask], RiakPid, Bucket) ->
	case riaktq_task:assign(RiakPid, Bucket, Id, Assignee) of
		{ok, T} ->
			In = riakc_map:fetch({<<"in">>, register}, T),
			Tags = case riakc_map:find({<<"tags">>, set}, T) of {ok, Val} -> Val; _ -> [] end,
			%% Dispatching the task to the instance
			riaktq_instance:put(Pid, [#{id => Id, in => In, tags => Tags}]),
			%% Move to a next instance and a next task
			handle_assign(Tinst, Ttask, RiakPid, Bucket);
		_  ->
			%% Try to dispatch a next message to the instance
			handle_assign(NextUpInstances, Ttask, RiakPid, Bucket)
	end;
handle_assign(_NextUpInstances, [], _RiakPid, _Bucket) -> ok;
handle_assign([], _TasksToDo, _RiakPid, _Bucket)       -> ok.

-spec handle_rollback(Ids, Ids, pid(), bucket_and_type()) -> ok when Ids :: gb_sets:set(binary()).
handle_rollback(TasksOnInstances, AssignedTasks, RiakPid, Bucket) ->
	handle_rollback(gb_sets:to_list(gb_sets:subtract(AssignedTasks, TasksOnInstances)), RiakPid, Bucket).

-spec handle_rollback([binary()], pid(), bucket_and_type()) -> ok.
handle_rollback([Id|T], RiakPid, Bucket) ->
	%% Rollback a state of the lost task
	_ = riaktq_task:rollback(RiakPid, Bucket, Id),
	handle_rollback(T, RiakPid, Bucket);
handle_rollback([], _RiakPid, _Bucket) ->
	ok.

-spec todo_tasks(pid(), binary()) -> [binary()].
todo_tasks(RiakPid, Index) ->
	riaktq_task:list(
		RiakPid,
		Index,
		#{fq => <<"status_register:todo">>,
			sort => <<"priority_register asc, cat_register asc">>}).

-spec assigned_tasks(pid(), binary()) -> [binary()].
assigned_tasks(RiakPid, Index) ->
	riaktq_task:list(RiakPid, Index, #{fq => <<"status_register:nextup AND assignee:*">>}).
