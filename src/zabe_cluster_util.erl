-module(zabe_cluster_util).
-compile([export_all]).
%% a tool for cluster test
%%
-define(HOST,'localhost').
get_node_name()->
    N=atom_to_list(node()),
    {match,[_,{S1,E1}]}= re:run(N,"[^@](.*)@.*",[]),
    "/tmp/"++string:substr(N,S1,E1+1).
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
start()->
    Nodes=['n1@localhost','n2@localhost','n3@localhost'],
    application:load(lager),
    Log=get_log(),
    Zab=get_zab(),
    Db=get_db(),
    os:cmd("rm -f "++ Log),
    os:cmd("rm -f "++ Zab),
    os:cmd("rm -f "++ Db),
    application:set_env(lager, handlers, [{lager_console_backend,error}
					  ,{lager_file_backend,[{Log,debug,10485760,"$D0",5}]}
					 ]),
    application:set_env(lager, error_logger_redirect, false),
 
    application:start(compiler),
    application:start(syntax_tools),
    application:start(lager),

    timer:sleep(2),
    zabe_proposal_leveldb_backend:start_link(Zab,[]),
    Op1=[{proposal_dir,Zab}],
    zabe_learn_leveldb:start_link(Nodes,Op1,Db).
start_slave(Name) ->
    Ps=code:get_path(),
    Arg=lists:foldl(fun(A,Acc)->
			    " -pa "++A++ Acc end, " ",Ps),
    slave:start(?HOST,Name,Arg),
    N1=list_to_atom(atom_to_list(Name)++"@"++atom_to_list(?HOST)),
    rpc:cast(N1,?MODULE,start,[])
	.


