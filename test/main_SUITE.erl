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

-module(main_SUITE).
-include_lib("common_test/include/ct.hrl").

-compile(export_all).

%% =============================================================================
%% Common Test callbacks
%% =============================================================================

all() ->
	application:ensure_all_started(riaktq),
	[{group, main}].

groups() ->
	[{main, [parallel], ct_helper:all(?MODULE)}].

init_per_suite(Config) ->
	riaktq_cth:init_config() ++ Config.

end_per_suite(Config) ->
	Config.

%% =============================================================================
%% Tests
%% =============================================================================

%% Result of newly created task is available after its execution.
open(Config) ->
	Tid = riaktq_cth:make_taskname(),
	Tinput = <<42>>,

	riaktq_cth:task_open(Tid, riaktq_task:new_dt(Tinput), Config),

	riaktq_cth:scheduler_wait(Config),
	Tobj = riaktq_cth:task_wait(Tid, Config),
	Tinput = riakc_map:fetch({<<"out">>, register}, Tobj),
	riaktq_cth:task_remove(Tid, Config).

%% If status of a task is changed to 'todo' (task is reopened) during its execution,
%% only result of recently created task will be available.
reopen_while_inprogress(Config) ->
	Interval = ?config(interval, Config),
	Tid = riaktq_cth:make_taskname(),
	Tinput = <<42>>,

	riaktq_cth:task_open(Tid, riaktq_task:new_dt(<<41>>), Config),
	timer:sleep(round(Interval *1.5)),
	riaktq_cth:task_open(Tid, riaktq_task:new_dt(Tinput), Config),

	riaktq_cth:scheduler_wait(Config),
	Tobj = riaktq_cth:task_wait(Tid, Config),
	Tinput = riakc_map:fetch({<<"out">>, register}, Tobj),
	riaktq_cth:task_remove(Tid, Config).

%% If status of a task is changed to 'todo' (task is reopened) after its completion,
%% result of recently created task will rewrite the previous one.
reopen_when_completed(Config) ->
	Interval = ?config(interval, Config),
	Tid = riaktq_cth:make_taskname(),
	Tinput = <<42>>,

	riaktq_cth:task_open(Tid, riaktq_task:new_dt(<<41>>), Config),
	timer:sleep(round(Interval *3)),
	riaktq_cth:task_open(Tid, riaktq_task:new_dt(Tinput), Config),

	riaktq_cth:scheduler_wait(Config),
	Tobj = riaktq_cth:task_wait(Tid, Config),
	Tinput = riakc_map:fetch({<<"out">>, register}, Tobj),
	riaktq_cth:task_remove(Tid, Config).

%% If a task was assigned but lost it will be reassigned.
assigned_but_lost(Config) ->
	Tid = riaktq_cth:make_taskname(),
	Tinput = <<42>>,
	Assignee = <<"agent">>,

	T0 = riaktq_task:new_dt(Tinput, [], <<"nextup">>, 0),
	T1 = riakc_map:update({<<"assignee">>, register}, fun(Obj) -> riakc_register:set(Assignee, Obj) end, T0),
	riaktq_cth:task_open(Tid, T1, Config),

	riaktq_cth:scheduler_wait(Config),
	Tobj = riaktq_cth:task_wait(Tid, Config),
	Tinput = riakc_map:fetch({<<"out">>, register}, Tobj),
	riaktq_cth:task_remove(Tid, Config).
