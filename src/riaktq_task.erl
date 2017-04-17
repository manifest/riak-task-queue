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

-module(riaktq_task).

-include_lib("riakc/include/riakc.hrl").

%% API
-export([
	list/2,
	list/3,
	open/4,
	rollback/3,
	rollback/4,
	assign/4,
	assign/5,
	close/5
]).

%% DataType API
-export([
	new_dt/1,
	new_dt/2,
	new_dt/4,
	new_dt/5
]).

%% Definitions
-define(DEFAULT_REQUEST_TIMEOUT, 5000).
-define(DEFAULT_REQUEST_ROWS, 20).
-define(TODO, <<"todo">>).
-define(NEXTUP, <<"nextup">>).
-define(FAILED, <<"failed">>).
-define(DONE, <<"done">>).

%% Types
-type task()   :: riakc_map:crdt_map().

-export_type([task/0]).

%% Callbacks
%% State is initialized once and then it's shared across the task handlers.
-callback init(Opts) -> {ok, State}
	when
		Opts :: any(),
		State :: any().

%% Results and errors:
%% 1. We return `{ok, binary()}` when task is succeed.
%%    Task's process will send `{riaktq_task_result, Id :: binary(), Status :: <<"done">>, Result :: binary()}`
%%    message to its parent process.
%% 2. We return `{error, binary()}` when it's possible to provide a readable reason of error.
%%    Task's process will send `{riaktq_task_result, Id :: binary(), Status :: <<"failed">>, Reason :: binary()}`
%%    message to its parent process.
%% 3. If any other value will be returned,
%%    task's process will exit with `{bad_return_value, Value}` reason.
%% 4. If task's process exit or will be killed,
%%    the message `{'DOWN', Ref, process, Pid, Reason}` will be sent to its parent process.
-callback handle(Id, Input, Tags, State) -> {ok, Output} | {error, Reason}
	when
		Id     :: binary(),
		Tags   :: [binary()],
		Input  :: binary(),
		Output :: binary(),
		Reason :: binary(),
		State  :: any().

%% Tags describe supported features of a task handler.
%% TODO: Task handler won't receive tasks with tags that are not members of the list.
-callback tags(State) -> Tags
	when
		Tags  :: [binary()],
		State :: any().

%% =============================================================================
%% API
%% =============================================================================

-spec list(pid(), binary()) -> [binary()].
list(Pid, Index) ->
	list(Pid, Index, #{}).

-spec list(pid(), binary(), map()) -> [binary()].
list(Pid, Index, Opts) ->
	Start = maps:get(start, Opts, 0),
	Rows = maps:get(rows, Opts, ?DEFAULT_REQUEST_ROWS),
	Query = maps:get(q, Opts, <<"*:*">>),
	Qopts =
		lists:foldl(
			fun
				({Key, {ok, Val}}, Acc) -> [{Key, Val}|Acc];
				(_, Acc)                -> Acc
			end,
			[{start, Start}, {rows, Rows}],
			[{filter, maps:find(fq, Opts)}, {sort, maps:find(sort, Opts)}]),

	case catch riakc_pb_socket:search(Pid, Index, Query, Qopts, ?DEFAULT_REQUEST_TIMEOUT) of
		{ok, {_, Docs, _, _}} ->
			[begin
				{_, Id} = lists:keyfind(<<"_yz_rk">>, 1, Doc),
				Id
			end || {_, Doc} <- Docs];
		{ok, _}          -> [];
		{error, Reason}  -> exit(Reason);
		{'EXIT', Reason} -> exit(Reason);
		Else             -> exit({bad_return_value, Else})
	end.

-spec open(pid(), bucket_and_type(), binary(), task()) -> ok.
open(Pid, Bucket, Id, Task) ->
	put(Pid, Bucket, Id, riakc_map:to_op(Task)).

-spec rollback(pid(), bucket_and_type(), binary()) -> {ok, task()} | {error, any()}.
rollback(Pid, Bucket, Id) ->
	rollback(Pid, Bucket, Id, fun(T) -> T end).

-spec rollback(pid(), bucket_and_type(), binary(), fun((riakc_datatype:datatype()) -> riakc_datatype:datatype())) -> {ok, task()} | {error, any()}.
rollback(Pid, Bucket, Id, Handle) ->
	case find_expected(Pid, Bucket, Id, ?NEXTUP) of
		{ok, T0} ->
			T1 = Handle(T0),
			T2 = riakc_map:update({<<"status">>, register}, fun(Obj) -> riakc_register:set(?TODO, Obj) end, T1),
			T3 = riakc_map:erase({<<"assignee">>, register}, T2),
			put(Pid, Bucket, Id, riakc_map:to_op(T3)),
			{ok, T3};
		ErrorReason ->
			ErrorReason
	end.

-spec assign(pid(), bucket_and_type(), binary(), binary()) -> {ok, task()} | {error, any()}.
assign(Pid, Bucket, Id, Assignee) ->
	assign(Pid, Bucket, Id, Assignee, fun(T) -> T end).

-spec assign(pid(), bucket_and_type(), binary(), binary(), fun((riakc_datatype:datatype()) -> riakc_datatype:datatype())) -> {ok, task()} | {error, any()}.
assign(Pid, Bucket, Id, Assignee, Handle) ->
	case find_expected(Pid, Bucket, Id, ?TODO) of
		{ok, T0} ->
			T1 = Handle(T0),
			T2 = riakc_map:update({<<"status">>, register}, fun(Obj) -> riakc_register:set(?NEXTUP, Obj) end, T1),
			T3 = riakc_map:update({<<"assignee">>, register}, fun(Obj) -> riakc_register:set(Assignee, Obj) end, T2),
			put(Pid, Bucket, Id, riakc_map:to_op(T3)),
			{ok, T3};
		ErrorReason ->
			ErrorReason
	end.

-spec close(pid(), bucket_and_type(), binary(), binary(), fun((riakc_datatype:datatype()) -> riakc_datatype:datatype())) -> {ok, task()} | {error, any()}.
close(Pid, Bucket, Id, Status, Handle) when Status =:= ?DONE; Status =:= ?FAILED ->
	case find_expected(Pid, Bucket, Id, ?NEXTUP) of
		{ok, T0} ->
			T1 = Handle(T0),
			T2 = riakc_map:update({<<"status">>, register}, fun(Obj) -> riakc_register:set(Status, Obj) end, T1),
			put(Pid, Bucket, Id, riakc_map:to_op(T2)),
			{ok, T2};
		ErrorReason ->
			ErrorReason
	end;
close(_Pid, _Bucket, _Id, Status, _Handle) ->
	{error, {bad_status, Status}}.

%% =============================================================================
%% DataType API
%% =============================================================================

-spec new_dt(binary()) -> task().
new_dt(Input) ->
	new_dt(Input, [], ?TODO, 0).

-spec new_dt(binary(), [binary()]) -> task().
new_dt(Input, Tags) ->
	new_dt(Input, Tags, ?TODO, 0).

-spec new_dt(binary(), [binary()], binary(), integer()) -> task().
new_dt(Input, Tags, Status, Priority) ->
	new_dt(Input, Tags, Status, Priority, riaktq:unix_time_us()).

-spec new_dt(binary(), [binary()], binary(), integer(), non_neg_integer()) -> task().
new_dt(Input, Tags, Status, Priority, CreatedAt) ->
	T0 = riakc_map:new(),
	T1 = riakc_map:update({<<"status">>, register}, fun(Obj) -> riakc_register:set(Status, Obj) end, T0),
	T2 = riakc_map:update({<<"priority">>, register}, fun(Obj) -> riakc_register:set(integer_to_binary(Priority), Obj) end, T1),
	T3 = riakc_map:update({<<"tags">>, set}, fun(Obj) -> riakc_set:add_elements(Tags, Obj) end, T2),
	T4 = riakc_map:update({<<"cat">>, register}, fun(Obj) -> riakc_register:set(integer_to_binary(CreatedAt), Obj) end, T3),
	T5 = riakc_map:update({<<"in">>, register}, fun(Obj) -> riakc_register:set(Input, Obj) end, T4),
	T5.

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec put(pid(), bucket_and_type(), binary(), riakc_datatype:update(any())) -> ok.
put(Pid, Bucket, Id, Op) ->
	case catch riakc_pb_socket:update_type(Pid, Bucket, Id, Op, [{pw, quorum}]) of
		ok                  -> ok;
		{error, unmodified} -> ok;
		{error, Reason}     -> exit(Reason);
		{'EXIT', Reason}    -> exit(Reason);
		Else                -> exit({bad_return_value, Else})
	end.

-spec find_expected(pid(), bucket_and_type(), binary(), binary()) -> {ok, task()} | {error, any()}.
find_expected(Pid, Bucket, Id, ExpectedStatus) ->
	case catch riakc_pb_socket:fetch_type(Pid, Bucket, Id, [{pr, quorum}]) of
		{ok, T} ->
			case riakc_map:fetch({<<"status">>, register}, T) of
				ExpectedStatus -> {ok, T};
				Status         -> {error, {nomatch_status, Status, ExpectedStatus}}
			end;
		{error, {notfound, _Type} =Reason} -> {error, Reason};
		{error, Reason}                    -> exit(Reason);
		{'EXIT', Reason}                   -> exit(Reason);
		Else                               -> exit({bad_return_value, Else})
	end.
