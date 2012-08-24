-module(zab_engine_gc_SUITE).
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
    os:cmd("rm -rf /tmp/*"),
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
    slave:stop('z1@localhost'),
    slave:stop('z2@localhost'),
    slave:stop('z3@localhost'),
    ok.

-define(HOST,'localhost').


all()->
    [gc].

gc(Config)->
    
    D1="/tmp/1.db",
    D2="/tmp/2.db",
    D3="/tmp/3.db",
    Pre="aaa",
    Op1=[{prefix,Pre},{proposal_dir,"/tmp/p1.db"},{gc_by_zab_log_count,3}],
    Op2=[{prefix,Pre},{proposal_dir,"/tmp/p2.db"},{gc_by_zab_log_count,3}],
    Op3=[{prefix,Pre},{proposal_dir,"/tmp/p3.db"},{gc_by_zab_log_count,3}],
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
    timer:sleep(300),


    {ok,_}=rpc:call('z1@localhost',zabe_log_gc_db,start_link,[]), 
    {ok,_}=rpc:call('z2@localhost',zabe_log_gc_db,start_link,[]),
    {ok,_}=rpc:call('z3@localhost',zabe_log_gc_db,start_link,[]),

    {ok,_}=rpc:call('z1@localhost',zabe_learn_leveldb,start_link,[Nodes,Op1,D1]), 
    {ok,_}=rpc:call('z2@localhost',zabe_learn_leveldb,start_link,[Nodes,Op2,D2]),
    {ok,_}=rpc:call('z3@localhost',zabe_learn_leveldb,start_link,[Nodes,Op3,D3]),
    timer:sleep(400),

    Key="test1",
    Value="value1",
    rpc:call('z1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    rpc:call('z1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    rpc:call('z1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    rpc:call('z1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    rpc:call('z1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    
    timer:sleep(500),
    {ok,[_Zxid]}=rpc:call('z1@localhost',zabe_log_gc_db,get_last_gc_zxid,[Pre]),
    ok
    .

