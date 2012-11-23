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
     gc,recover_gc_big_zxid
    ].
recover_gc_big_zxid(_C)->
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
    slave:stop('n1@localhost'),
    zabe_test_util:start_slave(n1,[{gc_by_zab_log_count,3}]),
    timer:sleep(3000),
    pang=net_adm:ping('n1@localhost'),
    error_logger:info_msg("~p~n",nodes()),
    zabe_test_util:stop(),
    ok.
    
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
    timer:sleep(5000),
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
    % restart follower
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



-define(TEST_DIR,"/tmp/").
-define(LAST_ZXID_KEY,<<"cen_last_zxid_key">>).
leader_crash_with_more_proposal(_C)->
    Bucket=1,
    D1=?TEST_DIR++"n1.db",
    D2=?TEST_DIR++"n2.db",
    D3=?TEST_DIR++"n3.db",
    Z1=?TEST_DIR++"n1.zab",
    Z2=?TEST_DIR++"n2.zab",
    Z3=?TEST_DIR++"n3.zab",
    os:cmd("rm -rf " ++ D1),
    os:cmd("rm -rf " ++ D2),
    os:cmd("rm -rf " ++ D3),
    os:cmd("rm -rf " ++ Z1),
    os:cmd("rm -rf " ++ Z2),
    os:cmd("rm -rf " ++ Z3),
    {ok,Zab1}=eleveldb:open(Z1, [{create_if_missing, true},{max_open_files,50},{comparator,zab}]),
    {ok,Zab2}=eleveldb:open(Z2, [{create_if_missing, true},{max_open_files,50},{comparator,zab}]),
    {ok,Zab3}=eleveldb:open(Z3, [{create_if_missing, true},{max_open_files,50},{comparator,zab}]),
    Log1=zabe_zxid:encode_key(Bucket,{1,1}),
    Log2=zabe_zxid:encode_key(Bucket,{1,2}),
    Log3=zabe_zxid:encode_key(Bucket,{1,3}),

    Proposal1=#p{sender=self(),client=self(),transaction=#t{zxid={1,1},value={put,"k1","v1"}}},
    Proposal2=#p{sender=self(),client=self(),transaction=#t{zxid={1,2},value={put,"k2","v2"}}},
    Proposal3=#p{sender=self(),client=self(),transaction=#t{zxid={1,3},value={put,"k3","v3"}}},
    eleveldb:put(Zab1,Log1,term_to_binary(Proposal1),[]),
    eleveldb:put(Zab1,Log2,term_to_binary(Proposal2),[]),
    eleveldb:put(Zab2,Log1,term_to_binary(Proposal1),[]),
    eleveldb:put(Zab2,Log2,term_to_binary(Proposal2),[]),
    eleveldb:put(Zab3,Log1,term_to_binary(Proposal1),[]),
    eleveldb:put(Zab3,Log2,term_to_binary(Proposal2),[]),
    %%leader has more proposal and crash 
    eleveldb:put(Zab3,Log3,term_to_binary(Proposal3),[]),
    {ok,_}=eleveldb:get(Zab3,Log3,[]),
    eleveldb:destroy(Z1,[]),
    eleveldb:destroy(Z2,[]),
    eleveldb:destroy(Z3,[]),
    {ok,Db1}=eleveldb:open(D1, [{create_if_missing, true},{max_open_files,50}]),
    {ok,Db2}=eleveldb:open(D2, [{create_if_missing, true},{max_open_files,50}]),
    {ok,Db3}=eleveldb:open(D3, [{create_if_missing, true},{max_open_files,50}]),
    eleveldb:put(Db1,?LAST_ZXID_KEY,term_to_binary({1,2}),[]),
    eleveldb:put(Db2,?LAST_ZXID_KEY,term_to_binary({1,2}),[]),
    eleveldb:put(Db3,?LAST_ZXID_KEY,term_to_binary({1,2}),[]),
    
    eleveldb:destroy(D1,[]),
    eleveldb:destroy(D2,[]),
    eleveldb:destroy(D3,[]),
    
    zabe_test_util:stop(),
    zabe_test_util:start_slave(n1),
    zabe_test_util:start_slave(n2),
    timer:sleep(2000),
    rpc:call('n1@localhost',zabe_learn_leveldb,put,["k4","v4"]),
    zabe_test_util:start_slave(n3),
    timer:sleep(3000),
    zabe_test_util:stop(),
    timer:sleep(30),
    {ok,Tmp}=eleveldb:open(Z3, [{create_if_missing, true},{max_open_files,50},{comparator,zab}]),
    %%leader should truncate Log3
    not_found=eleveldb:get(Tmp,Log3,[]),
    eleveldb:destroy(Z3,[]),
    ok.


