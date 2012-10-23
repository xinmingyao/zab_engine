-module(gen_zab_engine_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-compile([{parse_transform, lager_transform}]).
-include("zabe_main.hrl").
init_per_suite(Config)->
    Config    
.
 
end_per_suite(Config)->
    Config.

init_per_testcase(_TestCase, Config) ->
Config

    .

end_per_testcase(_TestCase,_Config) ->

    ok.



all()->
    [normal,leader_down_up,follow_down_up,partion,
     online_recover,
     gc
    ].
gc(_C)->
    zabe_test_util:stop(),
    zabe_test_util:start_slave(n1,[{gc_by_zab_log_count,3}]),
    zabe_test_util:start_slave(n2,[{gc_by_zab_log_count,3}]),
    zabe_test_util:start_slave(n3,[{gc_by_zab_log_count,3}]),
    timer:sleep(5000),
    Key="test1",
    Value="value1",
    rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    
    timer:sleep(500),
    {ok,_,Zxid1}=rpc:call('n1@localhost',zabe_proposal_leveldb_backend,get_gc,[[{bucket,1}]]),
    {ok,_,Zxid1}=rpc:call('n2@localhost',zabe_proposal_leveldb_backend,get_gc,[[{bucket,1}]]),
    {ok,_,Zxid1}=rpc:call('n3@localhost',zabe_proposal_leveldb_backend,get_gc,[[{bucket,1}]]),
    
    rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    {ok,_,Zxid2}=rpc:call('n1@localhost',zabe_proposal_leveldb_backend,get_gc,[[{bucket,1}]]),
    true = zabe_util:zxid_big(Zxid2,Zxid1).

normal(_C)->
    zabe_test_util:stop(),
    zabe_test_util:start_slave(n1),
    zabe_test_util:start_slave(n2),
    zabe_test_util:start_slave(n3),
    timer:sleep(2000),
    Key="test1",
    Value="value1",
    ok=rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    {ok,Value}=rpc:call('n1@localhost',zabe_learn_leveldb,get,[Key]),
    {ok,Value}=rpc:call('n2@localhost',zabe_learn_leveldb,get,[Key]),
    {ok,Value}=rpc:call('n3@localhost',zabe_learn_leveldb,get,[Key]),
    zabe_test_util:stop().

leader_down_up(_Config)-> 
    zabe_test_util:stop(),
    zabe_test_util:start_slave(n1),
    zabe_test_util:start_slave(n2),
    zabe_test_util:start_slave(n3),
    timer:sleep(2000),
    Key="test1",
    Value="value1",
    ok=rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    {ok,Value}=rpc:call('n1@localhost',zabe_learn_leveldb,get,[Key]),
    {ok,Value}=rpc:call('n2@localhost',zabe_learn_leveldb,get,[Key]),
    {ok,Value}=rpc:call('n3@localhost',zabe_learn_leveldb,get,[Key]),
    
    % stop leader ,leader was n3,see the fast elect
    slave:stop('n3@localhost'),
    timer:sleep(1000),
    ok=rpc:call('n1@localhost',zabe_learn_leveldb,put,["k2","v2"]),
    {ok,"v2"}=rpc:call('n2@localhost',zabe_learn_leveldb,get,["k2"]),
    % start n3
    zabe_test_util:start_slave(n3),
    timer:sleep(2000),
    {ok,"v2"}=rpc:call('n3@localhost',zabe_learn_leveldb,get,["k2"]),
    zabe_test_util:stop()
    

    .

follow_down_up(_Config)-> 
    zabe_test_util:stop(),
    zabe_test_util:start_slave(n1),
    zabe_test_util:start_slave(n2),
    zabe_test_util:start_slave(n3),
    timer:sleep(4000),
    Key="test1",
    Value="value1",
    ok=rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    {ok,Value}=rpc:call('n1@localhost',zabe_learn_leveldb,get,[Key]),
    {ok,Value}=rpc:call('n2@localhost',zabe_learn_leveldb,get,[Key]),
    {ok,Value}=rpc:call('n3@localhost',zabe_learn_leveldb,get,[Key]),
    
    % stop one follower
    slave:stop('n2@localhost'),
    ok=rpc:call('n1@localhost',zabe_learn_leveldb,put,["k2","v2"]),
    {ok,"v2"}=rpc:call('n3@localhost',zabe_learn_leveldb,get,["k2"]),
    % start n2
    zabe_test_util:start_slave(n2),
    timer:sleep(1000),
    {ok,"v2"}=rpc:call('n3@localhost',zabe_learn_leveldb,get,["k2"]),
    zabe_test_util:stop()
    .

partion(_C)->
    zabe_test_util:stop(),
    zabe_test_util:start_slave(n1),
    zabe_test_util:start_slave(n2),
    zabe_test_util:start_slave(n3),
    timer:sleep(3000),
    Key="test1",
    Value="value1",
    ok=rpc:call('n1@localhost',zabe_learn_leveldb,put,[Key,Value]),
    {ok,Value}=rpc:call('n1@localhost',zabe_learn_leveldb,get,[Key]),
    {ok,Value}=rpc:call('n2@localhost',zabe_learn_leveldb,get,[Key]),
    {ok,Value}=rpc:call('n3@localhost',zabe_learn_leveldb,get,[Key]),

    slave:stop('n1@localhost'),
    zabe_test_util:start_slave(n1),
    slave:stop('n2@localhost'),
    zabe_test_util:start_slave(n2),
    slave:stop('n3@localhost'),
    zabe_test_util:start_slave(n3),
    timer:sleep(3000),
    {ok,Value}=rpc:call('n1@localhost',zabe_learn_leveldb,get,[Key]),
    {ok,Value}=rpc:call('n2@localhost',zabe_learn_leveldb,get,[Key]),
    {ok,Value}=rpc:call('n3@localhost',zabe_learn_leveldb,get,[Key]),
    zabe_test_util:stop().

online_recover(_C)->
    zabe_test_util:stop(),
    zabe_test_util:start_slave(n1),
    zabe_test_util:start_slave(n3),
    ct:sleep(3000),
    lists:foreach(fun(A)->K=erlang:integer_to_list(A),
			  timer:sleep(2),
    			  rpc:call('n1@localhost',zabe_learn_leveldb,put,[K,K]) end,lists:seq(1,50)),
    ct:sleep(300),
    zabe_test_util:start_slave(n2),
    ct:sleep(2000),
    {ok,"1"}=rpc:call('n2@localhost',zabe_learn_leveldb,get,["1"]),
    {ok,"50"}=rpc:call('n2@localhost',zabe_learn_leveldb,get,["50"]),
    zabe_test_util:stop().


