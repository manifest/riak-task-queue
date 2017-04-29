-module(riaktq_eventm_task).
-behaviour(gen_event).

-include_lib("riakc/include/riakc.hrl").

%% API
-export([
	start_link/1,
	report_transition/5,
	subscribe/1,
	unsubscribe/1
]).

%% Gen Event callbacks
-export([
	init/1,
	handle_event/2,
	handle_call/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

%% Defintions
-define(REF, ?MODULE).

%% Types
-record(state, {
	listener :: pid()
}).

-type transition_event() :: {riaktq_task_transition, atom(), bucket_and_type(), binary(), riaktq_task:task()}.
-type event()            :: transition_event().

-export_type([event/0]).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link(atom()) -> {ok, pid()}.
start_link(Name) ->
	gen_event:start_link({local, Name}).

-spec report_transition(atom(), atom(), bucket_and_type(), binary(), riaktq_task:task()) -> ok.
report_transition(Ref, Transition, Bucket, Id, Task) ->
	gen_event:notify(Ref, {riaktq_task_transition, Transition, Bucket, Id, Task}).

-spec subscribe(atom()) -> gen_event:add_handler_ret().
subscribe(Ref) ->
	gen_event:add_sup_handler(Ref, {?MODULE, self()}, #{listener => self()}).

-spec unsubscribe(atom()) -> gen_event:del_handler_ret().
unsubscribe(Ref) ->
	gen_event:delete_handler(Ref, {?MODULE, self()}, {}).

%% =============================================================================
%% Gen Event callbacks
%% =============================================================================

init(#{listener := Pid}) ->
	{ok, #state{listener = Pid}}.

handle_event(Event, #state{listener = Pid} =State) ->
	Pid ! Event,
	{ok, State}.

handle_call(_Req, State) ->
	{ok, ok, State}.

handle_info(_Req, State) ->
	{ok, State}.

terminate(_Reason, State) ->
	{ok, State}.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
