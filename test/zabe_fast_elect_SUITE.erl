-module(zabe_fast_elect_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include("zabe_main.hrl").

init_per_suite(Config)->

    Config    
.


end_per_suite(Config)->

    Config.

init_per_testcase(_,Config)->

    Config
    .

end_per_testcase(_Config)->

    ok.



all()->
    [elect,elect_zxid_not_equal].

elect(_Config)->
    LastZxid={1,1},
    zabe_test_util:stop(),
    zabe_test_util:start_elect_slave(n1,LastZxid),
    zabe_test_util:start_elect_slave(n2,LastZxid),
    zabe_test_util:start_elect_slave(n3,LastZxid),    
    timer:sleep(1500),    

    ?FOLLOWING=rpc:call('n1@localhost',zabe_fast_leader_test,get_state,[]),
    ?FOLLOWING=rpc:call('n2@localhost',zabe_fast_leader_test,get_state,[]),
    ?LEADING=rpc:call('n3@localhost',zabe_fast_leader_test,get_state,[]),
    zabe_test_util:stop().

elect_zxid_not_equal(_C)->
    zabe_test_util:stop(),
    zabe_test_util:start_elect_slave(n1,{2,1}),
    zabe_test_util:start_elect_slave(n2,{1,1}),
    zabe_test_util:start_elect_slave(n3,{1,1}),    
    timer:sleep(1500),    
    ?LEADING=rpc:call('n1@localhost',zabe_fast_leader_test,get_state,[]),
    ?FOLLOWING=rpc:call('n2@localhost',zabe_fast_leader_test,get_state,[]),
    ?FOLLOWING=rpc:call('n3@localhost',zabe_fast_leader_test,get_state,[]),

    zabe_test_util:stop().
quorum(_C)->    
    LastZxid={1,1},
    zabe_test_util:stop(),
    zabe_test_util:start_elect_slave(n1,LastZxid),
    zabe_test_util:start_elect_slave(n2,LastZxid),    
    timer:sleep(1200),    
    ?FOLLOWING=rpc:call('n1@localhost',zabe_fast_leader_test,get_state,[]),
    ?LEADING=rpc:call('n2@localhost',zabe_fast_leader_test,get_state,[]),
    zabe_test_util:start_elect_slave(n3,LastZxid),    
    timer:sleep(1200),
    ?FOLLOWING=rpc:call('n3@localhost',zabe_fast_leader_test,get_state,[]).

    


