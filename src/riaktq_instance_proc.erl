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

-module(riaktq_instance_proc).

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

-spec run(binary(), binary(), [binary()], module(), any()) -> {pid(), reference()}.
run(Id, Data, Tags, Hmod, Hstate) ->
	spawn_monitor(?MODULE, loop, [self(), Id, Data, Tags, Hmod, Hstate]).

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec loop(pid(), binary(), binary(), [binary()], module(), any()) -> no_return().
loop(Parent, Id, Data, Tags, Hmod, Hstate) ->
	case Hmod:handle(Id, Data, Tags, Hstate) of
		{ok, Result}    -> Parent ! {riaktq_task_result, Id, <<"done">>, Result};
		{error, Reason} -> Parent ! {riaktq_task_result, Id, <<"failed">>, Reason};
		Else            -> exit({bad_return_value, Else})
	end.
