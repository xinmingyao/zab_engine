-module(zabe_fast_elect_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-compile([{parse_transform, lager_transform}]).
-include("zabe_main.hrl").


init_per_suite(Config)->
    application:start(consensus_engine),

    Config    
.



setup() ->
%    error_logger:tty(false),
    application:load(lager), 
    application:set_env(lager, handlers, [{lager_console_backend, info}
					  ,{lager_file_backend,[{"/tmp/console.log",info,10485760,"$D0",5}]}
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

end_per_testcase(Config)->

    ok.

-define(HOST,'localhost').


all()->
    [elect].

elect(Config)->
   % setup(),    
    Nodes=[node(),'n1@localhost','n2@localhost'],
    Q=2,
    LastZxid={1,1},
    Ps=code:get_path(),
    Arg=lists:foldl(fun(A,Acc)->
			    " -pa "++A++ Acc end, " ",Ps),
    slave:start(?HOST,n1,Arg),
    slave:start(?HOST,n2,Arg),
    setup(),
    rpc:call('n1@localhost',zabe_fast_elect_SUITE,setup,[]),
    rpc:call('n2@localhost',zabe_fast_elect_SUITE,setup,[]),
    zabe_fast_leader_test:start_link(LastZxid,Nodes,Q),
    
    rpc:call('n1@localhost',zabe_fast_leader_test,start_link,[LastZxid,Nodes,Q]),
    rpc:call('n2@localhost',zabe_fast_leader_test,start_link,[LastZxid,Nodes,Q]),
    ct:sleep(300),
    ?FOLLOWING=zabe_fast_leader_test:get_state(),
    ?FOLLOWING=rpc:call('n1@localhost',zabe_fast_leader_test,get_state,[]),
    ?LEADING=rpc:call('n2@localhost',zabe_fast_leader_test,get_state,[]),
    ct:sleep(200).

    

    


