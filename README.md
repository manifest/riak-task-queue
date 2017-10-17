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
- **retry**, number of restart attempts
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
Once per specified interval (1 minute by default), the scheduler does:
- commit completed tasks on instances, changing their status to `done` or `failed`
- restart tasks with `failed` status and `retry` > 0, lowering their `priority` by 1
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
TaskEventManager = riaktq_eventm_task,
supervisor:start_child(whereis(riaktq_sup), riaktq:eventm_task_spec(TaskEventManager)),
riaktq_eventm_task:subscribe(TaskEventManager),

%% Set scheduler's node name
SchedulerNode = 'tq-scheduler@127.0.0.1',
{ok, _} = net_kernel:start([SchedulerNode]),

%% Creating a scheduler and adding it to the supervision tree.
Bucket = {<<"riaktq_task_t">>, <<"task">>},
Index = <<"riaktq_task_idx">>,
Group = riaktq_instance_sup,
SchedulerConf =
  #{group => Group,
    riak_connection_pool => kv_protobuf,
    riak_bucket => Bucket,
    riak_index => Index,
    event_manager => TaskEventManager,
    interval => timer:seconds(5)},
supervisor:start_child(whereis(riaktq_sup), riaktq:scheduler_spec({scheduler, Group}, SchedulerConf)),

%% Creating five instances with `riaktq_echo` handler,
%% and adding them to the supervision tree.
[begin
  Name = <<"echo-", (integer_to_binary(N))/binary>>,
  InstanceConf =
    #{group => Group,
      name => Name,
      module => riaktq_echo,
      options => #{},
      transport_options => #{scheduler_node => SchedulerNode}},
  supervisor:start_child(whereis(Group), riaktq:instance_spec(Name, InstanceConf))
end || N <- lists:seq(1, 5)],

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
%%                   [{{<<"assignee">>,register},<<"echo-1">>},
%%                    {{<<"cat">>,register},<<"1506163346614292">>},
%%                    {{<<"in">>,register},<<"echo">>},
%%                    {{<<"priority">>,register},<<"0">>},
%%                    {{<<"retry">>,register},<<"0">>},
%%                    {{<<"status">>,register},<<"nextup">>}],
%%                   [],[],
%%                   <<131,108,0,0,0,1,104,2,109,0,0,0,12,35,9,254,249,6,117,
%%                     153,72,0,0,0,1,97,2,106>>}}
%% Shell got {riaktq_task_transition_data,close,
%%               {<<"riaktq_task_t">>,<<"task">>},
%%               <<"task-42">>,
%%               {map,
%%                   [{{<<"assignee">>,register},<<"echo-1">>},
%%                    {{<<"cat">>,register},<<"1506163346614292">>},
%%                    {{<<"in">>,register},<<"echo">>},
%%                    {{<<"laf">>,register},<<"16">>},
%%                    {{<<"out">>,register},<<"echo">>},
%%                    {{<<"priority">>,register},<<"0">>},
%%                    {{<<"retry">>,register},<<"0">>},
%%                    {{<<"sat">>,register},<<"1506163352875478">>},
%%                    {{<<"status">>,register},<<"done">>}],
%%                   [],[],
%%                   <<131,108,0,0,0,1,104,2,109,0,0,0,12,35,9,254,249,6,117,
%%                     153,72,0,0,0,1,97,3,106>>}}

%% We could register an event manager to receive daily reports at specified time.
QueryEventManager = riaktq_eventm_query,
supervisor:start_child(whereis(riaktq_sup), riaktq:eventm_query_spec(QueryEventManager)),
riaktq_eventm_task:subscribe(QueryEventManager),

ObserveTime = {0,0,0}, %% 00:00:00
ObserveQueries =
  [ #{key => <<"tasks-that-done">>, status => <<"done">>},
    #{key => <<"tasks-that-created-at-least-1second-before">>, age => 1} ],
ObserverConf =
  #{riak_connection_pool => kv_protobuf,
    riak_index => Index,
    event_manager => QueryEventManager,
    queries => ObserveQueries,
    time => ObserveTime,
    interval => timer:seconds(5)},
supervisor:start_child(whereis(riaktq_sup), riaktq:observer_spec(observer, ObserverConf)).

flush().
%% Shell got {riaktq_query_result,<<"tasks-that-done">>,[<<"task-42">>]}
%% Shell got {riaktq_query_result,<<"tasks-that-created-at-least-1second-before">>,
%%                                [<<"task-42">>]}
```



### License

The source code is provided under the terms of [the MIT license][license].

[license]:http://www.opensource.org/licenses/MIT
[travis]:https://travis-ci.org/manifest/riak-task-queue?branch=master
[travis-img]:https://secure.travis-ci.org/manifest/riak-task-queue.png?branch=master
[riak-task-queue-task-status-img]:misc/task-status.png
