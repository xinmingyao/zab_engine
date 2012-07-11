-module(elect2_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-compile([{parse_transform, lager_transform}]).
-include("zabe_main.hrl").

%% test one node join cluster and become follower
%%
%%
%%

init_per_suite(Config)->
    application:start(consensus_engine),

    Config    
.



setup() ->
%    error_logger:tty(false),
    application:load(lager), 
    application:set_env(lager, handlers, [{lager_console_backend,error}
					  ,{lager_file_backend,[{"/tmp/console.log",error,10485760,"$D0",5}]}
					 ]),
    application:set_env(lager, error_logger_redirect, false),
    application:start(compiler),
    application:start(syntax_tools),
    application:start(lager)
   % gen_event:call(lager_event, ?MODULE, flush)
.

cleanup(_) ->
    application:stop(lager),
    error_logger:tty(true).

end_per_suite(Config)->

    Config.

init_per_testcase(_,Config)->

    Config
    .

end_per_testcase(_Config)->
    erlang:exit(whereis(zabe_fast_leader_test),normal),
    slave:stop('z1@localhost'),
    slave:stop('z2@localhost'),
    ok.

-define(HOST,'localhost').


all()->
    [elect].

elect(Config)->
   % setup(),    
    Nodes=[node(),'z1@localhost','z2@localhost'],
    Q=2,
    LastZxid={1,1},
    Ps=code:get_path(),
    Arg=lists:foldl(fun(A,Acc)->
			    " -pa "++A++ Acc end, " ",Ps),
    slave:start(?HOST,z1,Arg),
    slave:start(?HOST,z2,Arg),
    setup(),
    rpc:call('z1@localhost',zabe_fast_elect_SUITE,setup,[]),
    rpc:call('z2@localhost',zabe_fast_elect_SUITE,setup,[]),
    zabe_fast_leader_test:start_link(LastZxid,Nodes,Q),
    
    rpc:call('z1@localhost',zabe_fast_leader_test,start_link,[LastZxid,Nodes,Q]),
    rpc:call('z2@localhost',zabe_fast_leader_test,start_link,[LastZxid,Nodes,Q]),
    ct:sleep(200),
    ?FOLLOWING=zabe_fast_leader_test:get_state(),
    ?LEADING=rpc:call('z2@localhost',zabe_fast_leader_test,get_state,[]),
    ?FOLLOWING=rpc:call('z1@localhost',zabe_fast_leader_test,get_state,[]),
    ct:sleep(10).

    

    


