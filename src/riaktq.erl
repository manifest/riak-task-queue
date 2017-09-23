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

-module(riaktq).

%% API
-export([
	scheduler_spec/2,
	instance_spec/2,
	observer_spec/2,
	eventm_task_spec/1,
	eventm_query_spec/1,
	unix_time/0,
	unix_time/1,
	unix_time_us/0,
	unix_time_us/1
]).

%% Definitions
-define(APP, ?MODULE).

%% =============================================================================
%% API
%% =============================================================================

-spec scheduler_spec(any(), map()) -> supervisor:child_spec().
scheduler_spec(Ref, Conf) ->
  #{id => Ref,
    start => {riaktq_scheduler, start_link, [Conf]},
    restart => permanent}.

-spec instance_spec(binary(), map()) -> supervisor:child_spec().
instance_spec(Ref, Conf) ->
  #{id => Ref,
    start => {riaktq_instance, start_link, [Conf]},
    restart => permanent}.

-spec observer_spec(any(), map()) -> supervisor:child_spec().
observer_spec(Ref, Conf) ->
  #{id => Ref,
    start => {riaktq_observer, start_link, [Conf]},
    restart => permanent}.

-spec eventm_task_spec(atom()) -> supervisor:child_spec().
eventm_task_spec(Ref) ->
  #{id => Ref,
    start => {riaktq_eventm_task, start_link, [Ref]},
    restart => permanent}.

-spec eventm_query_spec(atom()) -> supervisor:child_spec().
eventm_query_spec(Ref) ->
  #{id => Ref,
    start => {riaktq_eventm_query, start_link, [Ref]},
    restart => permanent}.

-spec unix_time() -> non_neg_integer().
unix_time() ->
	unix_time(erlang:timestamp()).

-spec unix_time(erlang:timestamp()) -> non_neg_integer().
unix_time({MS, S, _US}) ->
	MS * 1000000 + S.

-spec unix_time_us() -> non_neg_integer().
unix_time_us() ->
	unix_time_us(erlang:timestamp()).

-spec unix_time_us(erlang:timestamp()) -> non_neg_integer().
unix_time_us({MS, S, US}) ->
	MS * 1000000000000 + S * 1000000 + US.
