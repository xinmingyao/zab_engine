-module(follow_online_recover_SUITE).
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
    application:set_env(lager, handlers, [{lager_console_backend, error}
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
   %  os:cmd("rm -rf /tmp/proposal.bb"),
   % timer:sleep(2),
   % {ok,Pid}=zabe_proposal_leveldb_backend:start_link("/tmp/proposal.bb",[]),
   % [{pid,Pid}|Config]
Config

    .

end_per_testcase(_TestCase, Config) ->
    catch erlang:exit(whereis(zabe_learn_leveldb),normal),
    catch slave:stop('z1@localhost'),
    catch slave:stop('z2@localhost'),
    catch slave:stop('z3@localhost'),
  %  Pid=?config(pid,Config),
  %  erlang:exit(Pid,normal),        
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

    {ok,_}=rpc:call('z3@localhost',zabe_learn_leveldb,start_link,[Nodes,Op3,D3]),
    timer:sleep(800),

    Key="test1",
    Value="value1",
    rpc:call('z1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    
    {ok,_}=rpc:call('z2@localhost',zabe_learn_leveldb,start_link,[Nodes,Op2,D2]),
   
     lists:foreach(fun(A)->K=erlang:integer_to_list(A),
			  timer:sleep(2),
    			  rpc:call('z1@localhost',zabe_learn_leveldb,put,[K,K]) end,lists:seq(1,50)),
 
    {ok,Value}=rpc:call('z1@localhost',zabe_learn_leveldb,get,[Key]),

    {ok,Value}=rpc:call('z3@localhost',zabe_learn_leveldb,get,[Key]),

   
    ct:sleep(2000),
    {ok,Value}=rpc:call('z2@localhost',zabe_learn_leveldb,get,[Key]),
    {ok,"1"}=rpc:call('z2@localhost',zabe_learn_leveldb,get,["1"]),
    {ok,"50"}=rpc:call('z2@localhost',zabe_learn_leveldb,get,["50"]),
    ok
    .

