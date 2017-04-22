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

To execute a task, we simply put it to Riak KV using `riaktq_task:open/4` function.
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
  #{name => riaktq_riakc,
    size => 5,
    connection =>
      #{host => "192.168.99.100",
        port => 8087,
        options => [queue_if_disconnected]}},
supervisor:start_child(whereis(riaktq_sup), riakc_pool:child_spec(RiakPoolConf)),

%% Creating a scheduler and adding it to the supervision tree.
Bucket = {<<"riaktq_task_t">>, <<"task">>},
Index = <<"riaktq_task_idx">>,
SchedulerConf =
  #{riak_connection_pool => riaktq_riakc,
    riak_bucket => Bucket,
    riak_index => Index,
    schedule_interval => timer:seconds(5)},
supervisor:start_child(whereis(riaktq_sup), riaktq:child_spec(SchedulerConf)),

%% Creating five instances with `riaktq_echo` handler,
%% and adding them to the supervision tree.
InstanceConf =
  #{module => riaktq_echo,
    options => #{}},
[ supervisor:start_child(
    whereis(riaktq_instance_sup),
    riaktq:instance_child_spec(<<"echo-", (integer_to_binary(N))/binary>>, InstanceConf))
  || N <- lists:seq(1, 5) ],

%% Opening a new task
Pid = riakc_pool:lock(riaktq_riakc),
riaktq_task:open(Pid, Bucket, <<"task-1">>, riaktq_task:new_dt(<<"echo">>)).

%% Getting result
{ok, Task1} = riakc_pb_socket:fetch_type(Pid, Bucket, <<"task-1">>),
riakc_map:fetch({<<"out">>, register}, Task1).
%% <<"echo">>
```



### License

The source code is provided under the terms of [the MIT license][license].

[license]:http://www.opensource.org/licenses/MIT
[travis]:https://travis-ci.org/manifest/riak-task-queue?branch=master
[travis-img]:https://secure.travis-ci.org/manifest/riak-task-queue.png?branch=master
[riak-task-queue-task-status-img]:misc/task-status.png
