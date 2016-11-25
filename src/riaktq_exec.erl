%% ----------------------------------------------------------------------------
%% The MIT License
%%
%% Copyright (c) 2016 Andrei Nesterov <ae.nesterov@gmail.com>
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

-module(riaktq_exec).
-behaviour(riaktq_task).

%% API
-export([
	init/1,
	handle/4,
	tags/1
]).

%% =============================================================================
%% API
%% =============================================================================

-spec init(any()) -> {ok, any()}.
init(_Opts) ->
	{ok, ignore}.

-spec handle(binary(), binary(), [binary()], any()) -> {ok | error, binary()}.
handle(_Id, Cmd, _Tags, _State) ->
	{Status, Result} = exec:run(binary_to_list(Cmd), [sync, stdout, stderr]),
	StdOut = case lists:keyfind(stdout, 1, Result) of {_, StdOut0} -> StdOut0; _ -> [] end,
	StdErr = case lists:keyfind(stderr, 1, Result) of {_, StdErr0} -> StdErr0; _ -> [] end,
	ExitStatus =
		case lists:keyfind(exit_status, 1, Result) of
			{_, ExitStatus0} -> {_, ExitStatus1} = exec:status(ExitStatus0), ExitStatus1;
			_                -> 0
		end,

	{	Status,
		jsx:encode(
			#{stdout => StdOut,
				stderr => StdErr,
				exit_status => ExitStatus}) }.

-spec tags(any()) -> [binary()].
tags(_State) ->
	[<<"exec">>].
