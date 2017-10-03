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

-module(riaktq_instance).
-behaviour(gen_statem).

-include("riaktq_log.hrl").

%% API
-export([
	start_link/1,
	list/1,
	get/1,
	put/2,
	commit/2
]).

%% GenStateM callbacks
-export([
	init/1,
	handle_event/4,
	callback_mode/0,
	terminate/3,
	code_change/4
]).

%% Types
-type input() ::
	#{id   => binary(),
		in   => binary(),
		tags => [binary()]}.

-type output() ::
	#{id => binary(),
		status => binary(),
		out => binary(),
		sat => non_neg_integer(),
		laf => non_neg_integer()}.

-record(proc, {
	pid :: pid(),
	ref :: reference(),
	sat :: non_neg_integer()
}).

-type proc() :: #proc{}.

-record(state, {
	hproc  :: proc() | undefined,
	name   :: binary(),
	hmod   :: module(),
	hstate :: any(),
	in     :: queue:queue(input()),
	out    :: queue:queue(output())
}).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link(map()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Conf) ->
	gen_statem:start_link(?MODULE, Conf, []).

-spec list(atom()) -> [pid()].
list(Group) ->
	try
		pg2:create(Group),
		pg2:get_members(Group)
	catch T:R ->
		?ERROR_REPORT([{group, Group}], T, R),
		[]
	end.

-spec get(pid()) -> map().
get(Pid) ->
	gen_statem:call(Pid, {get}).

-spec put(pid(), [input()]) -> ok.
put(Pid, Input) ->
	gen_statem:cast(Pid, {put, Input}).

-spec commit(pid(), [binary()]) -> ok.
commit(Pid, Ids) ->
	gen_statem:cast(Pid, {commit, Ids}).

%% =============================================================================
%% GenStateM callbacks
%% =============================================================================

init(Conf) ->
	Name = validate_name(Conf),
	Hmod = validate_module(Conf),
	Hopts = validate_options(Conf, #{}),
	connect(validate_group(Conf), validate_transport_options(Conf)),

	{ok, Hstate} = Hmod:init(Hopts),
	Sdata =
		#state{
			name = Name,
			hmod = Hmod,
			hstate = Hstate,
			in = queue:new(),
			out = queue:new()},

	{ok, idle, Sdata}.

%% Somebody would like to get results and status of the instance: reply to them.
handle_event({call, From}, {get}, Sname, #state{in=Qin, out=Qout, name=Name}) ->
	{keep_state_and_data, [{reply, From, handle_get(Sname, Qin, Qout, Name)}]};
%% We have just got a few task: we will try to execute them soon.
handle_event(cast, {put, Input}, _Sname, #state{in=Qin} =Sdata) ->
	{keep_state, Sdata#state{in=handle_put(Input, Qin)}, [{next_event, internal, try_run_task}]};
%% Results are confirmed by scheduler: remove them from instance.
handle_event(cast, {commit, Ids}, _Sname, #state{out=Qout} =Sdata) ->
	{keep_state, Sdata#state{out=handle_commit(Ids, Qout)}};
%% We don't have any running task at the moment (hproc=undefined),
%% - and there are tasks in the input queue: run first and switch to the 'busy' state.
%% - and input queue is empty: switch to the 'idle' state.
handle_event(internal, try_run_task, _Sname, #state{in=Qin, hmod=Hmod, hstate=Hstate, hproc=undefined} =Sdata) ->
	case queue:peek(Qin) of
		{value, #{id := Id, in := Data, tags := Tags}} ->
			{Hpid, Href} = riaktq_instance_proc:run(Id, Data, Tags, Hmod, Hstate),
			{next_state, busy, Sdata#state{hproc=#proc{pid=Hpid,ref=Href,sat=riaktq:unix_time_us()}}};
		_ ->
			{next_state, idle, Sdata}
	end;
%% Task is done or failed: commit the result, and move to a next task.
handle_event(info, {riaktq_task_result, _Id, Status, Data}, busy, #state{in=Qin0, out=Qout0, hproc=#proc{ref=Href, sat=Hsat}} =Sdata) ->
	demonitor(Href, [flush]),
	{Qin1, Qout1} = handle_result(Hsat, Status, Data, Qin0, Qout0),
	{keep_state, Sdata#state{in=Qin1, out=Qout1, hproc=undefined}, [{next_event, internal, try_run_task}]};
%% Task process is failed (Reason=/=normal): commit the error reason as a result, and move to a next task.
handle_event(info, {'DOWN', Href, process, _Pid, Reason}, busy, #state{in=Qin0, out=Qout0, hproc=#proc{ref=Href, sat=Hsat}} =Sdata) when Reason =/= normal ->
	Data = list_to_binary(lists:flatten(io_lib:format("~p", [Reason]))),
	{Qin1, Qout1} = handle_result(Hsat, <<"failed">>, Data, Qin0, Qout0),
	{keep_state, Sdata#state{in=Qin1, out=Qout1, hproc=undefined}, [{next_event, internal, try_run_task}]};
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

-spec connect(atom(), map()) -> ok.
connect(Group, #{scheduler_node := Node}) ->
	Pid = self(),
	true = net_kernel:connect_node(Node),
	pg2:create(Group),
	case lists:member(Pid, pg2:get_members(Group)) of
		true -> ok;
		_    -> ok = pg2:join(Group, self())
	end.

-spec handle_put([input()], Qin) -> Qin when Qin :: queue:queue(input()).
handle_put([Input|T], Qin) ->
	try 
		#{id := Id, in := In, tags := Tags} = Input,
		true = is_binary(Id),
		true = is_binary(In),
		[true = is_binary(Tag) || Tag <- Tags],
		handle_put(T, queue:in(#{id => Id, in => In, tags => Tags}, Qin))
	catch _:_ ->
		?WARNING_REPORT([{reason, bad_input}, {input, Input}]),
		handle_put(T, Qin)
	end;
handle_put([], Qin) ->
	Qin.

-spec handle_commit([binary()], Qout) -> Qout when Qout :: queue:queue(output()).
handle_commit(Ids, Qout) ->
	queue:filter(fun(#{id := Id}) -> not lists:member(Id, Ids)  end, Qout).

-spec handle_result(non_neg_integer(), binary(), binary(), Qin, Qout) -> {Qin, Qout}
	when Qin :: queue:queue(input()), Qout :: queue:queue(output()).
handle_result(Hstart, Status, Data, Qin0, Qout0) ->
	Hlasted = riaktq:unix_time_us() - Hstart,
	{{value, #{id := Id}}, Qin1} = queue:out(Qin0),
	Val =
		#{id => Id,
			status => Status,
			out => Data,
			sat => Hstart,
			laf => Hlasted},
	{Qin1, queue:in(Val, Qout0)}.

-spec handle_get(any(), queue:queue(input()), queue:queue(output()), binary()) -> map().
handle_get(Status, Qin, Qout, Name) ->
	#{status => atom_to_binary(Status, utf8),
		output => queue:to_list(Qout),
		input => queue:to_list(Qin),
		name => Name}.

-spec validate_module(map()) -> module().
validate_module(#{module := Val}) when is_atom(Val) -> Val;
validate_module(#{module := Val})                   -> error({invalid_module, Val});
validate_module(_)                                  -> error(missing_module).

-spec validate_options(map(), map()) -> map().
validate_options(#{options := Val}, _) when is_map(Val) -> Val;
validate_options(#{options := Val}, _)                  -> error({invalid_options, Val});
validate_options(_, Default)                            -> Default.

-spec validate_name(map()) -> binary().
validate_name(#{name := Val}) when is_binary(Val) -> Val;
validate_name(#{name := Val})                     -> error({invalid_name, Val});
validate_name(_)                                  -> error(missing_name).

-spec validate_group(map()) -> atom().
validate_group(#{group := Val}) when is_atom(Val) -> Val;
validate_group(#{group := Val})                   -> error({invalid_group, Val});
validate_group(_)                                 -> error(missing_group).

-spec validate_transport_options(map()) -> map().
validate_transport_options(#{transport_options := Val}) ->
	validate_transport_scheduler_node(Val),
	Val;
validate_transport_options(_) ->
	error(missing_transport_options).

-spec validate_transport_scheduler_node(map()) -> node().
validate_transport_scheduler_node(#{scheduler_node := Val}) when is_atom(Val) -> Val;
validate_transport_scheduler_node(#{scheduler_node := Val})                   -> error({invalid_transport_scheduler_node, Val});
validate_transport_scheduler_node(_)                                          -> error(missing_transport_scheduler_node).
