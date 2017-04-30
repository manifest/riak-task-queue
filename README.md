# Riak Task Queue

[![Build Status][travis-img]][travis]

Persistent task queue with scheduler and multiple worker instances on top of Riak KV.



### Overview

#### Tasks

Above all, we have **tasks**. They describe the following properties:
- **status**:
	- `todo`, for the newly created tasks
	- `nextup`, for tasks that have already been scheduled for execution
	- `done` or `failed`, for complete tasks
- **tags** (not implemented), features of the instance required for the task execution
- **priority**, priority of the task
- **assignee**, instance that have been chosen for the task execution
- **in**, task's input
- **out**, output of the task
- **laf**, duration of the task execution
- **sat**, time when task was started
- **cat**, time when task was created

![task-status][riak-task-queue-task-status-img]

#### Worker Instances

We have worker instances to execute tasks.
Each of them manage its own input queue of tasks, executing one task per time in arrival order.
When task is complete, it's moved to the output queue. The task remains there until it would be commited by scheduler.

#### Scheduler

To execute a task, we simply put it to Riak KV using `riaktq_task:open/{4,5}` functions.
Once per specified interval (1 minute by default), the scheduler:
- commit tasks on instances that have already completed changing their status to `done` or `failed`
- assign tasks with `todo` status to instances for future execution, changing their status to `nextup`
- rollback lost tasks to `todo` status (in case of instance failure)

Note that, the scheduler won't start a next iteration while the previous one in progress
and there is could be many instances but only one scheduler.



### How To Use

To build and start playing with the library, execute following commands in the shell:

```bash
## Building the development image and running the container with Riak KV within it.
$ ./run-docker.sh
## Building the application and executing an erlang shell.
$ make app shell
```

Here is a minimal example:

```erlang
%% Creating a pool of Riak KV connections and adding it to the supervision tree.
RiakPoolConf =
  #{name => kv_protobuf,
    size => 5,
    connection =>
      #{host => "192.168.99.100",
        port => 8087,
        options => [queue_if_disconnected]}},
supervisor:start_child(whereis(riaktq_sup), riakc_pool:child_spec(RiakPoolConf)),

%% We could register an event manager to receive state transitions of tasks.
EventManager = riaktq_eventm,
supervisor:start_child(whereis(riaktq_sup), riaktq:eventm_task_spec(EventManager)),
riaktq_eventm_task:subscribe(EventManager),

%% Creating a scheduler and adding it to the supervision tree.
Bucket = {<<"riaktq_task_t">>, <<"task">>},
Index = <<"riaktq_task_idx">>,
Group = riaktq_instance_sup,
SchedulerConf =
  #{group => Group,
    riak_connection_pool => kv_protobuf,
    riak_bucket => Bucket,
    riak_index => Index,
    event_manager => EventManager,
    schedule_interval => timer:seconds(5)},
supervisor:start_child(whereis(riaktq_sup), riaktq:scheduler_spec(SchedulerConf)),

%% Creating five instances with `riaktq_echo` handler,
%% and adding them to the supervision tree.
InstanceConf =
  #{module => riaktq_echo,
    options => #{}},
[ supervisor:start_child(
    whereis(Group),
    riaktq:instance_spec(<<"echo-", (integer_to_binary(N))/binary>>, InstanceConf))
  || N <- lists:seq(1, 5) ],

%% Opening a new task
Pid = riakc_pool:lock(kv_protobuf),
riaktq_task:open(Pid, Bucket, <<"task-42">>, riaktq_task:new_dt(<<"echo">>)).

%% Getting result
Task = riaktq_task:get(Pid, Bucket, <<"task-42">>),
riakc_map:fetch({<<"out">>, register}, Task).
%% <<"echo">>

flush().
%% Shell got {riaktq_task_transition_data,assign,
%%               {<<"riaktq_task_t">>,<<"task">>},
%%               <<"task-42">>,
%%               {map,
%%                   [{{<<"cat">>,register},<<"1493552444228474">>},
%%                    {{<<"in">>,register},<<"echo">>},
%%                    {{<<"priority">>,register},<<"0">>},
%%                    {{<<"status">>,register},<<"todo">>}],
%%                   [{{<<"assignee">>,register},{register,<<>>,<<"echo-1">>}},
%%                    {{<<"status">>,register},
%%                     {register,<<"todo">>,<<"nextup">>}}],
%%                   [],
%%                   <<131,108,0,0,0,1,104,2,109,0,0,0,12,35,9,254,249,150,165,
%%                     185,51,0,0,0,1,97,1,106>>}}
%% Shell got {riaktq_task_transition_data,close,
%%               {<<"riaktq_task_t">>,<<"task">>},
%%               <<"task-42">>,
%%               {map,
%%                   [{{<<"assignee">>,register},<<"echo-1">>},
%%                    {{<<"cat">>,register},<<"1493552444228474">>},
%%                    {{<<"in">>,register},<<"echo">>},
%%                    {{<<"priority">>,register},<<"0">>},
%%                    {{<<"status">>,register},<<"nextup">>}],
%%                   [{{<<"laf">>,register},{register,<<>>,<<"18">>}},
%%                    {{<<"out">>,register},{register,<<>>,<<"echo">>}},
%%                    {{<<"sat">>,register},
%%                     {register,<<>>,<<"1493552449284949">>}},
%%                    {{<<"status">>,register},
%%                     {register,<<"nextup">>,<<"done">>}}],
%%                   [],
%%                   <<131,108,0,0,0,1,104,2,109,0,0,0,12,35,9,254,249,150,165,
%%                     185,51,0,0,0,1,97,2,106>>}}
```



### License

The source code is provided under the terms of [the MIT license][license].

[license]:http://www.opensource.org/licenses/MIT
[travis]:https://travis-ci.org/manifest/riak-task-queue?branch=master
[travis-img]:https://secure.travis-ci.org/manifest/riak-task-queue.png?branch=master
[riak-task-queue-task-status-img]:misc/task-status.png
