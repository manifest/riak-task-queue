# Riak Task Queue

Task queue on top of Riak KV



### How To Use

Build and run the docker container with Riak KV within it.

```bash
$ ./run-docker.sh
```

To build and start playing with the library, execute following commands in the shell:

```bash
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
riaktq_task:open(Pid, Bucket, <<"task-1">>, riaktq_task:new(<<"echo">>)).

%% Getting result
{ok, Task1} = riakc_pb_socket:fetch_type(Pid, Bucket, <<"task-1">>),
riakc_map:fetch({<<"out">>, register}, Task1).
%% <<"echo">>
```



### License

The source code is provided under the terms of [the MIT license][license].

[license]:http://www.opensource.org/licenses/MIT
