-module(zabe_test_util).
-compile([export_all]).
%% a tool for cluster test
%%
-define(HOST,'localhost').
-define(NODES,['n1@localhost','n2@localhost','n3@localhost']).

-define(TEST_DIR,"/tmp/").
get_node_name()->
    N=atom_to_list(node()),
    {match,[_,{S1,E1}]}= re:run(N,"[^@](.*)@.*",[]),
    ?TEST_DIR++string:substr(N,S1,E1+1).
get_db()->	
    get_node_name()++".db".
get_log()->
    get_node_name()++".log".
get_zab()->
    get_node_name()++".zab".

stop()->
    slave:stop('n1@localhost'),
    slave:stop('n2@localhost'),
    slave:stop('n3@localhost').
start(Opts)->
    
    Zab=get_zab(),
    Db=get_db(),

    os:cmd("rm -f "++ Zab),
    os:cmd("rm -f "++ Db),
    
    start_lager(),
    timer:sleep(2),
    zabe_proposal_leveldb_backend:start_link(Zab,[]),
    Op1=[{proposal_dir,Zab}|Opts],
    zabe_learn_leveldb:start_link(?NODES,Op1,Db).


start_lager()->
    Log=get_log(),
    os:cmd("rm -f "++ Log),
    application:load(lager),
    application:set_env(lager, handlers, [{lager_console_backend,error}
					  ,{lager_file_backend,[{Log,debug,10485760,"$D0",5}]}
					 ]),
    application:set_env(lager, error_logger_redirect, false),
 
    application:start(compiler),
    application:start(syntax_tools),
    application:start(lager).
start_slave(Name,Opts) ->
    slave:start(?HOST,Name,get_code_path()),
    N1=list_to_atom(atom_to_list(Name)++"@"++atom_to_list(?HOST)),
    rpc:cast(N1,?MODULE,start,[Opts])
	.

start_slave(Name) ->
    slave:start(?HOST,Name,get_code_path()),
    N1=list_to_atom(atom_to_list(Name)++"@"++atom_to_list(?HOST)),
    rpc:cast(N1,?MODULE,start,[[]])
	.

start_elect_slave(Name,LastZxid)->
    slave:start(?HOST,Name,get_code_path()),
    N1=list_to_atom(atom_to_list(Name)++"@"++atom_to_list(?HOST)),
    rpc:cast(N1,?MODULE,start_elect_slave2,[LastZxid]).

start_elect_slave2(LastZxid)->
    Q=2,
    start_lager(),
    zabe_fast_leader_test:start_link(LastZxid,?NODES,Q).
get_code_path()->
    Ps=code:get_path(),
    lists:foldl(fun(A,Acc)->
			    " -pa "++A++ Acc end, " ",Ps).
    
