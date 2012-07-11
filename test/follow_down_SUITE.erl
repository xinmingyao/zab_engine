-module(follow_down_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-compile([{parse_transform, lager_transform}]).
-include("zabe_main.hrl").

init_per_suite(Config)->
    Config    
.

setup(Dir) ->
%    error_logger:tty(false),
    application:load(lager), 
    application:set_env(lager, handlers, [{lager_console_backend,error}
					  ,{lager_file_backend,[{"/tmp/console.log",error,10485760,"$D0",5}]}
					 ]),
    application:set_env(lager, error_logger_redirect, false),
    application:start(compiler),
    application:start(syntax_tools),
    application:start(lager),
    os:cmd("rm -rf /tmp/*.db"),
    timer:sleep(2),
    zabe_proposal_leveldb_backend:start_link(Dir,[]),
 ok   
.

cleanup(_) ->
    application:stop(lager),
    error_logger:tty(true).

end_per_suite(Config)->

    Config.


init_per_testcase(_TestCase, Config) ->

Config

    .

end_per_testcase(_TestCase, Config) ->
  %  Pid=?config(pid,Config),
  %  erlang:exit(Pid,normal),        
  catch  erlang:exit(whereis(zabe_learn_leveldb),normal),
  %  slave:stop('z1@localhost'),
    slave:stop('z2@localhost'),
    slave:stop('z3@localhost'),
    ok.

-define(HOST,'localhost').


all()->
    [elect].

elect(Config)->

    D1="/tmp/1.db",
    D2="/tmp/2.db",
    D3="/tmp/3.db",
    Op1=[{proposal_dir,"/tmp/p1.db"}],
    Op2=[{proposal_dir,"/tmp/p2.db"}],
    Op3=[{proposal_dir,"/tmp/p3.db"}],
    Nodes=['z1@localhost','z2@localhost','z3@localhost'],
    Ps=code:get_path(),
    Arg=lists:foldl(fun(A,Acc)->
			    " -pa "++A++ Acc end, " ",Ps),
    slave:start(?HOST,z1,Arg),
    slave:start(?HOST,z2,Arg),
    slave:start(?HOST,z3,Arg),
    
    ok=rpc:call('z1@localhost',?MODULE,setup,["/tmp/p1.db"]),
    ok=rpc:call('z2@localhost',?MODULE,setup,["/tmp/p2.db"]),
    ok=rpc:call('z3@localhost',?MODULE,setup,["/tmp/p3.db"]),
    timer:sleep(500),

    {ok,_}=rpc:call('z1@localhost',zabe_learn_leveldb,start_link,[Nodes,Op1,D1]), 
    {ok,_}=rpc:call('z2@localhost',zabe_learn_leveldb,start_link,[Nodes,Op2,D2]),
    {ok,_}=rpc:call('z3@localhost',zabe_learn_leveldb,start_link,[Nodes,Op3,D3]),
    timer:sleep(800),

    Key="test1",
    Value="value1",
    rpc:call('z1@localhost',zabe_learn_leveldb,put,[Key,Value]),
 
   % ok=zabe_learn_leveldb:put("k2","v2"),
   % ok=zabe_learn_leveldb:put("k3","v3"),
    %{ok,Value}=zabe_learn_leveldb:get(Key),

%    {ok,Value}=rpc:call('z2@localhost',zabe_learn_leveldb,get,[Key]),
%    {ok,Value}=rpc:call('z1@localhost',zabe_learn_leveldb,get,[Key]),
%   {ok,"v3"}=rpc:call('z1@localhost',zabe_learn_leveldb,get,["k3"]),
    slave:stop('z2@localhost'),
    
    {ok,Value}=rpc:call('z1@localhost',zabe_learn_leveldb,get,[Key]),
     slave:stop('z1@localhost'),
    {ok,Value}=rpc:call('z3@localhost',zabe_learn_leveldb,get,[Key]),
    {error,not_ready}=rpc:call('z3@localhost',zabe_learn_leveldb,put,[Key,Value]),
    slave:start(?HOST,z2,Arg),
    rpc:call('z2@localhost',?MODULE,setup,["/tmp/p2.db"]),
    ct:sleep(1000),
    rpc:call('z2@localhost',zabe_learn_leveldb,start_link,[Nodes,Op2,D2]),
    ct:sleep(1000),
    ok=rpc:call('z3@localhost',zabe_learn_leveldb,put,[Key,Value]),
    ok
    .

