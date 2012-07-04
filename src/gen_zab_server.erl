

%% state: init->looking->leader_recover->leading
%%                       follow_recover->following    ->end

-module(gen_zab_server).

%% Time between rounds of query from the leader
-define(TAU,5000).

-export([start/6,
         start_link/6,
         proposal_call/2, proposal_call/3, proposal_cast/2,
         call/2, call/3, cast/2,
         reply/2]).
-export([system_continue/3,
         system_terminate/4,
         system_code_change/4,
         format_status/2

        ]).


%% Internal exports
-export([init_it/6,
         print_event/3
        ]).

-export([loop/3,looking/4,leader_recover/4,follow_recover/4,following/4,leading/4]).
-export([send_zab_msg/2]).



%-define(LEADER_STATE_LOOKING,1).
%-define(LEADER_STATE_RECORVER,2).
%-define(LEADER_STATE_LEADING,3).
%-define(LEADER_STATE_FOLLOWING,4).
%-define(LEADER_STATE_OBSERVEING,5).


-compile([{parse_transform, lager_transform}]).

-include("zabe_main.hrl").
-type option() :: {'workers',    Workers::[node()]}
                | {'proposal_dir',     Dir::string()}
                | {'heartbeat',  Seconds::integer()}.

-type options() :: [option()].
%% A locally registered name
-type name() :: atom().
-type server_ref() :: name() | {name(),node()} | {global,name()} | pid().
%% See gen_server.
-type caller_ref() :: {pid(), reference()}.
%% Opaque state of the gen_leader behaviour.

-record(server, {
          parent,
          mod,
          state,
	  debug,
	  role::?LEADING|?FOLLOWING,
	  proposal_que::ets,
	  last_zxid::zxid(),
	  current_zxid::zxid(),
	  ensemble::[node()|_],
	  quorum::non_neg_integer(),
	  last_commit_zxid,
	  proposal_log_mod,
	  leader::node(),
	  observers::[],
	  elect_pid::pid(),
	  live_nodes::dict:new(),
	  recover_acks::dict:new(),
	  mon_leader_ref::reference(),
	  mon_follow_refs::dict:new()
         }).

%%% ---------------------------------------------------
%%% Interface functions.
%%% ---------------------------------------------------



-callback init(Opts::[])->
    ok.
-callback handle_commit(Request::any(),Zxid::zxid(),State::any(),ZabServerInfo::#zab_server_info{})->
    {ok,Result::any(),NewState::any()}.
-callback handle_call(Msg::any(),From::any(),State::any(),ZabServerInfo::#zab_server_info{})->
    {ok,Result::any(),NewState::any()}.

-callback handle_cast(Msg::any(),State::any(),ZabServerInfo::#zab_server_info{})->
    {ok,NewState::any()}.

-callback handle_info(Msg::any(),State::any(),ZabServerInfo::#zab_server_info{})->
    {ok,NewState::any()}.
 
-callback terminate(Reason::any(),State::any(),ZabServerInfo::#zab_server_info{})->
    {ok,NewState::any()}.
-callback code_change(Msg::any(),From::any(),State::any(),ZabServerInfo::#zab_server_info{})->
    {ok,NewState::any()}.



-type start_ret() :: {'ok', pid()} | {'error', term()}.

%% @doc Starts a gen_leader process without linking to the parent.
%% @see start_link/6
-spec start(Name::atom(), CandidateNodes::[node()], OptArgs::options(),
            Mod::module(), Arg::term(), Options::list()) -> start_ret().
start(Name, CandidateNodes, OptArgs, Mod, Arg, Options)
  when is_atom(Name), is_list(CandidateNodes), is_list(OptArgs) ->
    gen:start(?MODULE, nolink, {local,Name},
              Mod, {CandidateNodes, OptArgs, Arg}, Options).


-spec start_link(Name::atom(), CandidateNodes::[node()], OptArgs::options(),
            Mod::module(), Arg::term(), Options::list()) -> start_ret().
start_link(Name, CandidateNodes, OptArgs, Mod, Arg, Options)
  when is_atom(Name), is_list(CandidateNodes), is_list(OptArgs) ->
    gen:start(?MODULE, link, {local,Name},
              Mod, {CandidateNodes, OptArgs, Arg}, Options).

%%
%% Make a call to a generic server.
%% If the server is located at another node, that node will
%% be monitored.
%% If the client is trapping exits and is linked server termination
%% is handled here (? Shall we do that here (or rely on timeouts) ?).
%%
%% @doc Equivalent to <code>gen_server:call/2</code>, but with a slightly
%% different exit reason if something goes wrong. This function calls
%% the <code>gen_leader</code> process exactly as if it were a gen_server
%% (which, for practical purposes, it is.)
%% @end
-spec call(server_ref(), term()) -> term().
call(Name, Request) ->
    case catch gen:call(Name, '$gen_call', Request) of
        {ok,Res} ->
            Res;
        {'EXIT',Reason} ->
            exit({Reason, {?MODULE, local_call, [Name, Request]}})
    end.

%% @doc Equivalent to <code>gen_server:call/3</code>, but with a slightly
%% different exit reason if something goes wrong. This function calls
%% the <code>gen_leader</code> process exactly as if it were a gen_server
%% (which, for practical purposes, it is.)
%% @end
-spec call(server_ref(), term(), integer()) -> term().
call(Name, Request, Timeout) ->
    case catch gen:call(Name, '$gen_call', Request, Timeout) of
        {ok,Res} ->
            Res;
        {'EXIT',Reason} ->
            exit({Reason, {?MODULE, local_call, [Name, Request, Timeout]}})
    end.

%% @doc Makes a call (similar to <code>gen_server:call/2</code>) to the
%% leader. The call is forwarded via the local gen_leader instance, if
%% that one isn't actually the leader. The client will exit if the
%% leader dies while the request is outstanding.
%% <p>This function uses <code>gen:call/3</code>, and is subject to the
%% same default timeout as e.g. <code>gen_server:call/2</code>.</p>
%% @end
%%
-spec proposal_call(Name::server_ref(), Request::term()) -> term().
proposal_call(Name, Request) ->
    case catch gen:call(Name, '$proposal_call', Request) of
        
        {ok,Res={error,not_ready}} ->
            Res;
	{ok,Res} ->
            Res;
        {'EXIT',Reason} ->
            exit({Reason, {?MODULE, proposal_call, [Name, Request]}})
    end.

%% @doc Makes a call (similar to <code>gen_server:call/3</code>) to the
%% leader. The call is forwarded via the local gen_leader instance, if
%% that one isn't actually the leader. The client will exit if the
%% leader dies while the request is outstanding.
%% @end
%%
-spec proposal_call(Name::server_ref(), Request::term(),
                  Timeout::integer()) -> term().
proposal_call(Name, Request, Timeout) ->
    case catch gen:call(Name, '$proposal_call', Request, Timeout) of
        {ok,{leader,reply,Res}} ->
            Res;
        {'EXIT',Reason} ->
            exit({Reason, {?MODULE, leader_call, [Name, Request, Timeout]}})
    end.


%% @equiv gen_server:cast/2
-spec cast(Name::name()|pid(), Request::term()) -> 'ok'.
cast(Name, Request) ->
    catch do_cast('$gen_cast', Name, Request),
    ok.

%% @doc Similar to <code>gen_server:cast/2</code> but will be forwarded to
%% the leader via the local gen_leader instance.
-spec proposal_cast(Name::name()|pid(), Request::term()) -> 'ok'.
proposal_cast(Name, Request) ->
    catch do_cast('$proposal_cast', Name, Request),
    ok.


do_cast(Tag, Name, Request) when is_atom(Name) ->
    Name ! {Tag, Request};
do_cast(Tag, Pid, Request) when is_pid(Pid) ->
    Pid ! {Tag, Request}.


%% @equiv gen_server:reply/2
-spec reply(From::caller_ref(), Reply::term()) -> term().
reply({To, Tag}, Reply) ->
    catch To ! {Tag, Reply}.


%%% ---------------------------------------------------
%%% Initiate the new process.
%%% Register the name using the Rfunc function
%%% Calls the Mod:init/Args function.
%%% Finally an acknowledge is sent to Parent and the main
%%% loop is entered.
%%% ---------------------------------------------------
%%% @hidden
init_it(Starter, Parent, {local, Name}, Mod, {CandidateNodes, Workers, Arg}, Options) ->
    %% R13B passes {local, Name} instead of just Name
    init_it(Starter, Parent, Name, Mod,
            {CandidateNodes, Workers, Arg}, Options);
init_it(Starter, self, Name, Mod, {CandidateNodes, OptArgs, Arg}, Options) ->
    init_it(Starter, self(), Name, Mod,
            {CandidateNodes, OptArgs, Arg}, Options);
init_it(Starter,Parent,Name,Mod,{CandidateNodes,OptArgs,Arg},Options) ->
%    Interval    = proplists:get_value(heartbeat, OptArgs, ?TAU div 1000) * 1000,
    ElectMod        = proplists:get_value(elect_mod,      OptArgs,zabe_fast_elect),
    ProposalLogMod        = proplists:get_value(proposal_log_mod,      OptArgs,zabe_proposal_leveldb_backend),
    Debug       = debug_options(Name, Options),
%    ProposalDir =proplists:get_value(proposal_dir,OptArgs,"/tmp/p1.ldb"),
     case Mod:init(Arg)  of
        {stop, Reason} ->
            proc_lib:init_ack(Starter, {error, Reason}),
            exit(Reason);
        ignore ->
            proc_lib:init_ack(Starter, ignore),
            exit(normal);
        {'EXIT', Reason} ->
            proc_lib:init_ack(Starter, {error, Reason}),
            exit(Reason);
        {ok, State,LastCommitZxid} ->
	     proc_lib:init_ack(Starter, {ok, self()}),
	     Ensemble=CandidateNodes,
	     Quorum=ordsets:size(Ensemble) div  2  +1,
	     LastZxid= case ProposalLogMod:get_last_proposal([]) of
			   {ok,not_found}->{0,0};
			   {ok,Z}->Z
		       end,

	     Election=#election{parent=Mod,last_zxid=LastZxid,ensemble=Ensemble,quorum=Quorum,last_commit_zxid=LastCommitZxid},
	     
	     {ok,Pid}=ElectMod:start_link(Election),
	     Que=ets:new(list_to_atom(atom_to_list(Mod)++"_que"),[{keypos,2},ordered_set]),
	     loop(#server{parent = Parent,mod = Mod,elect_pid=Pid,
				  ensemble=Ensemble,
				  last_commit_zxid=LastCommitZxid,
			          recover_acks=dict:new(),
			  mon_follow_refs=dict:new(),
				  state = State,last_zxid=LastZxid,current_zxid=LastZxid,proposal_log_mod=ProposalLogMod,
				  debug = Debug,quorum=Quorum,proposal_que=Que},looking,#zab_server_info{}
			 )
            ;
        Else ->
	     Error = {init_bad_return_value, Else},
	     proc_lib:init_ack(Starter, {error, Error}),
	     exit(Error)
    end.

%%% ---------------------------------------------------
%%% The MAIN loops.
%%% ---------------------------------------------------

loop(Server=#server{debug=Debug,elect_pid=EPid,last_zxid=LastZxid,
		    last_commit_zxid=LastCommitZxid,proposal_que=Que,
		    mon_follow_refs=MonFollowRefs,quorum=Quorum,
		    mon_leader_ref=ModLeaderRef},ZabState,ZabServerInfo)->
    receive
	Msg1->
	    lager:info("zab state:~p receive msg:~p",[ZabState,Msg1]),
	    case Msg1 of
		{'DOWN',ModLeaderRef,_,_,_}->
		    ets:delete_all_objects(Que),
		    
		    gen_fsm:send_event(EPid,{re_elect,LastZxid,LastCommitZxid}),
		    lager:info("zxid:~p,commit zxid:~p,change state~p to looking",[LastZxid,LastCommitZxid,ZabState]),
		    loop(Server#server{
			    recover_acks=dict:new(),
			    mon_follow_refs=dict:new()
			   },looking,#zab_server_info{}
			 )
		    ;
		{'DOWN',_MRef,process,F={_Mod,_Node},_} when ZabState=:=leading->
		    N1=dict:erase(F,MonFollowRefs),

		    S1=dict:size(N1)+1 ,
		    if S1>=Quorum
		       ->loop(Server#server{mon_follow_refs=N1},ZabState,ZabServerInfo);
		       true->
			    ets:delete_all_objects(Que),
			    lager:info("zxid:~p,commit zxid:~p,change state~p to looking",[LastZxid,LastCommitZxid,ZabState]),
			    gen_fsm:send_event(EPid,{re_elect,LastZxid,LastCommitZxid}),
			    loop(Server#server{
				    recover_acks=dict:new(),
				    mon_follow_refs=dict:new()
				   },looking,#zab_server_info{}
				)
		    end
			     
		    ;
		    
		#msg{cmd=?VOTE_CMD,value=V} ->
		    gen_fsm:send_event(EPid,V),
		    loop(Server,ZabState,ZabServerInfo);
		%#msg{cmd=?VOTE_CMD,value=V}->
		%    V=erlang:get(vote),
		%    V1=V#vote{from=node(),leader=V#vote.leader,zxid=LastZxid},
		%    send_zab_msg(V#vote.from,V1), 
		% 
		%    loop(Server,ZabState,ZabServerInfo);
		#msg{cmd=?ZAB_CMD,value=Value}->
		    ?MODULE:ZabState(Server,Value,ZabState,ZabServerInfo);
		{'$proposal_call',From,_Msg} when (ZabState =/=leading andalso  ZabState =/=following) ->
		    reply(From,{error,not_ready}),
		   loop(Server,ZabState,ZabServerInfo);
		V={'$proposal_call',_From,_Msg}->
		    ?MODULE:ZabState(Server,V,ZabState,ZabServerInfo);
		{'$proposal_cast',_From,_Msg} when (ZabState =/=?LEADING andalso  ZabState =/=?FOLLOWING) ->
		   flush,do_nothing,
		   loop(Server,ZabState,ZabServerInfo);
		V={'$proposal_cast',_From,_Msg}->
		    ?MODULE:ZabState(Server,V,ZabState);
		_ when Debug == [] ->
		    handle_msg(Msg1, Server,ZabState,ZabServerInfo);
		_ ->
		    Debug1 = sys:handle_debug(Debug, {?MODULE, print_event},
				      "nouse", {in, Msg1}),
		    handle_msg(Msg1, Server#server{debug = Debug1},ZabState, ZabServerInfo)
	    end
    end.
		    

looking(#server{mod = Mod, state = State,debug=_Debug,quorum=Quorum,elect_pid=EPid,
		last_zxid=LastZxid,last_commit_zxid=LastCommitZxid,
		mon_follow_refs=MonFollowRefs,
		proposal_que=Que,
		proposal_log_mod=ProposalLogMod} = Server
	,Msg1,ZabState,ZabServerInfo)->
    case Msg1 of
	{elect_reply,{ok,V=#vote{leader=Node},RecvVotes}} when Node=:=node() ->	    
	    erlang:put(vote,V),
	    Last=case zabe_util:zxid_eq(LastZxid,LastCommitZxid) of
		true -> LastCommitZxid;
		false->ProposalLogMod:fold(fun({_Key,P1},_Acc)->
						   
					   Mod:handle_commit(P1#proposal.transaction#transaction.value,
							     P1#proposal.transaction#transaction.zxid,State,ZabServerInfo) ,
						   P1#proposal.transaction#transaction.zxid
					   end,
					   LastCommitZxid,LastCommitZxid)
	    end,
	    
	    %%monitor_follows(RecvVotes,Quorum),
	    %%
	    %lager:info("monitors ~p~p",[MonFollowRefs,RecvVotes]),
	    
	    N5=lists:foldl(fun(#vote{from=From},Acc)->

				   if From=:=node() ->
					   Acc;
				      true->
					   case catch erlang:monitor(process,{Mod,From}) of
					       {'EXIT',_}->
						   Acc;
					       Ref->
						   dict:store({Mod,From},Ref,Acc) end end end,MonFollowRefs,RecvVotes),
	    T6=dict:size(N5)+1,
	    if T6 >=Quorum 
	       ->
	
	    %%
		    loop(Server#server{role=?LEADING,leader=Node,last_zxid=Last,mon_follow_refs=N5,
				       last_commit_zxid=Last},leader_recover,ZabServerInfo) ;
	       true-> 
		    ets:delete_all_objects(Que),
			    gen_fsm:send_event(EPid,{re_elect,LastZxid,LastCommitZxid}),
			    loop(Server#server{
				    recover_acks=dict:new(),
				    mon_follow_refs=dict:new()
				   },looking,#zab_server_info{}
				)
	    end;
	{elect_reply,{ok,V=#vote{leader=Node,zxid=_LeaderZxid},_}}  -> 
	    erlang:put(vote,V),
	    monitor_leader(Mod,Node),
	    case catch erlang:monitor(process,{Mod,Node}) of
		{'EXIT',_}->
		    restart_elect;
		Ref->
       %	    case zabe_util:zxid_compare(LeaderZxid,LastZxid) of
       %		epoch_small1->
		    timer:sleep(50),%%todo fix this by leader handle this
		    M1={truncate_req,{Mod,node()},LastZxid},
		    
		    send_zab_msg({Mod,Node},M1),
		    loop(Server#server{leader=Node,role=?FOLLOWING,mon_leader_ref=Ref},follow_recover,ZabServerInfo) 
	    end;
	 
		
		
	%	equal->
        %		    timer:sleep(50), %%leader maybe slow,and in looking state todo fix this,use timer send to leader
	%	    M1={recover_ok,{Mod,node()}},
	%	    send_zab_msg({Mod,Node},M1),
        %		    loop(Server#server{leader=Node,role=?FOLLOWING},following,ZabServerInfo) ;
	%	_->
	%	    timer:sleep(50),
	%	    M1={recover_req,{Mod,node()},LastZxid},
        %		    send_zab_msg({Mod,Node},M1),
	%	    loop(Server#server{leader=Node,role=?FOLLOWING},follow_recover,ZabServerInfo)
	%    end;
	    
	 _ -> %flush msg
	    loop(Server,ZabState,ZabServerInfo) 
    end
.


leader_recover(#server{mod = _Mod, state = _State,debug=_Debug,quorum=Quorum,elect_pid=_EPid,
		last_zxid=LastZxid,last_commit_zxid=_LastCommitZxid,recover_acks=RecoverAcks,
		proposal_log_mod=ProposalLogMod,
		       current_zxid=CurZxid,mon_follow_refs=MRefs
		      } = Server
	,Msg1,ZabState,ZabServerInfo)->
    case Msg1 of
	{recover_ok,From} ->	    
	    D1=dict:store(From,"",RecoverAcks),
	    NRefs=case dict:is_key(From,MRefs) of
		true->MRefs;
		false->
		   case catch erlang:monitor(process,From) of
				    {'EXIT',_}->
					MRefs;
				    Ref->
					dict:store(From,Ref,MRefs)
		   end
	    end,
	    Z=dict:size(D1)+1,
	    if Z>=Quorum ->
		    lager:info("leader recover change state to leading"),
		    %%change epoch for handle old leader restart
		    Z1=zabe_util:change_leader_zxid(LastZxid),
		    loop(Server#server{recover_acks=D1,mon_follow_refs=NRefs,last_zxid=Z1,last_commit_zxid=Z1},leading,ZabServerInfo);
	       true->
		    loop(Server#server{recover_acks=D1,mon_follow_refs=NRefs},ZabState,ZabServerInfo)
	    end;
	{truncate_req,From,{Epoch1,_}}  -> 
	    {LeaderEpoch,_}=LastZxid,
	    EpochLastZxid=if LeaderEpoch > Epoch1
	       ->{ok,EL}=ProposalLogMod:get_epoch_last_zxid(Epoch1,[]),EL;
	       true -> 
		    not_need
	    end,	    	    
	    M1={truncate_ack,EpochLastZxid},
	    send_zab_msg(From,M1),
	    loop(Server,ZabState,ZabServerInfo);

	{recover_req,From,StartZxid} ->
	    {ok,Res}=ProposalLogMod:iterate_zxid_count(fun({_K,V})->
				       V end,StartZxid,100),
	    
	    M1={recover_ack,Res,CurZxid},
	    
	    send_zab_msg(From,M1),
	    loop(Server,ZabState,ZabServerInfo);
	_ -> %flush msg
	    loop(Server,ZabState,ZabServerInfo) 
    end
.

follow_recover(#server{mod =Mod, state = State,quorum=_Quorum,leader=Leader,
		last_zxid=LastZxid,last_commit_zxid=_LastCommitZxid,recover_acks=_RecoverAcks,
		proposal_que=Que,
		proposal_log_mod=ProposalLogMod} = Server
	,Msg1,ZabState,ZabServerInfo)->
    case Msg1 of
	{recover_ack,Proposals,_LeaderCurrentZxid} ->
	    Last=lists:foldl(fun(Proposal,_)->
			      ProposalLogMod:put_proposal(Proposal#proposal.transaction#transaction.zxid,Proposal,[]),
			      Mod:handle_commit(Proposal#proposal.transaction#transaction.value,
						Proposal#proposal.transaction#transaction.zxid,State,ZabServerInfo),
				Proposal#proposal.transaction#transaction.zxid
		      end ,LastZxid,Proposals),
	    QueSize=ets:info(Que,size),
	   
	    case Proposals of
		[] when QueSize=:=0->
		    M1={recover_ok,{Mod,node()}},
		    send_zab_msg({Mod,Leader},M1),
		    lager:info("follow recover ok,state to followiing"),
		    loop(Server,following,ZabServerInfo);
		[] when QueSize>0-> %recover local

		    F=fun(P1)->
				      Z1=P1#proposal_rec.proposal#proposal.transaction#transaction.zxid,
				      ProposalLogMod:put_proposal(Z1,P1#proposal_rec.proposal,[]),
				      Mod:handle_commit(P1#proposal_rec.proposal,Z1,State,ZabServerInfo),
							 Z1
						  end,
			   % NewZxid=fold_all(Que,F,Last),
		    NewZxid=fold_all(Que,F,Last,ets:first(Que),LastZxid),
		    lager:info("follow recover local ok,state to followiing"),
		    loop(Server#server{last_zxid=NewZxid,last_commit_zxid=NewZxid},following,ZabServerInfo)    
			;

		_ when QueSize=:=0->
		    M1={recover_req,{Mod,node()},zabe_util:zxid_plus(Last)},
			    send_zab_msg({Mod,Leader},M1),
		    loop(Server,ZabState,ZabServerInfo);
		_ when QueSize>0 ->
		    Min=ets:first(Que),
		   % [M2|_]=ets:lookup(Que,Min),
		   % Pro=M2#proposal_rec.proposal,
		    % MinZxid = Pro#proposal.transaction#transaction.zxid,
                    case zabe_util:zxid_big_eq(Last,Min) of
			true->		
			   % lager:info("recover local~p",[ets:tab2list(Que)]),
			   % lager:info(" recover local ~p ~p",[Last,MinZxid]),
			    F=fun(P1)->
				      Z1=P1#proposal_rec.proposal#proposal.transaction#transaction.zxid,
				      ProposalLogMod:put_proposal(Z1,P1#proposal_rec.proposal,[]),
				      Mod:handle_commit(P1#proposal_rec.proposal,Z1,State,ZabServerInfo),
							 Z1
						  end,
			   % NewZxid=fold_all(Que,F,Last),
			    NewZxid=fold_all(Que,F,Last,Min,LastZxid),
			    %ets:delete_all_objects(Que),
			    lager:info("follow recover ok,state to followiing"),
			    loop(Server#server{last_zxid=NewZxid,last_commit_zxid=NewZxid},following,ZabServerInfo)    
				;
			false ->
			    
			    M1={recover_req,{Mod,node()},zabe_util:zxid_plus(Last)},
			    send_zab_msg({Mod,Leader},M1),
			    loop(Server,ZabState,ZabServerInfo)
		    end
	    end
		;
	{truncate_ack,LeaderEpochLastZxid}  ->
	    {E1,_}=LastZxid,
	    MZxid=case LeaderEpochLastZxid of
		      not_need->zabe_util:zxid_plus(LastZxid);
		      not_found->{E1+1,1};
		_->
			  
			  {ok,L1}=ProposalLogMod:trunc(fun({K,_V})->
							       K end ,zabe_util:zxid_plus(LeaderEpochLastZxid),[]),
			  lists:map(fun(Key)->
					    ProposalLogMod:delete_proposal(Key,[]) end,L1),
			  {E1+1,1}
	    end,
	    M1={recover_req,{Mod,node()},MZxid},
	    send_zab_msg({Mod,Leader},M1),
	    loop(Server,ZabState,ZabServerInfo);
	#zab_req{msg=Msg}->
	    Zxid1=Msg#proposal.transaction#transaction.zxid,
	    ets:insert(Que,#proposal_rec{zxid=Zxid1,proposal=Msg,commit=false}),
	    loop(Server,ZabState,ZabServerInfo);
	#zab_commit{msg=Zxid1}->
	    [P1|_]=ets:lookup(Que,Zxid1),
	    ets:insert(Que,P1#proposal_rec{commit=true}),
	    loop(Server,ZabState,ZabServerInfo);
	_ -> %flush msg %todo save proposal msg into que
	    loop(Server,ZabState,ZabServerInfo) 
    end
.
			  
fold_all(Que,F,Last,Key,Acc)->
 case ets:lookup(Que,Key) of
     [P1|_]->
	 lager:info("dfdfd~p",[zabe_util:zxid_big_eq(Last,Key)]),
	 case zabe_util:zxid_big_eq(Last,Key) of
	     true->
		 ets:delete(Que,Key),
		 fold_all(Que,F,Last,zabe_util:zxid_plus(Key),Acc);
	     _->
		 case P1#proposal_rec.commit of
		     true->						%ets:delete(Que,Key#proposal_rec.zxid),
			 Acc1=F(P1),
			 ets:delete(Que,Key),
			 fold_all(Que,F,Last,zabe_util:zxid_plus(Key),Acc1);
		     false ->
			 Acc
		 end
	 end;
     _->Acc
 end
.

monitor_leader(Mod,Node)->
    
    
    todo,
    Proc={?MODULE,Node},
    Proc.
monitor_follows(_RecvRotes,_Quorum)->
    todo,
    %ParentId=self(),
    %spawn a new process to monitor follows,if live follows < quorum 
    %send {error,lost_quorum} to 
    ok.



following(#server{mod = Mod, state = State,
		     ensemble=_Assemble,
		     proposal_log_mod=ProposalLogMod,elect_pid=_EPid,
		     quorum=_Quorum,debug=_Debug,last_zxid=_Zxid,proposal_que=_Que,leader=Leader} = Server,Msg1,ZabState,ZabServerInfo)->

    case Msg1 of
	{'$proposal_call',From,Msg} ->
	    send_zab_msg({Mod,Leader},{zab_proposal,From,Msg,{Mod,node()}}),
	    loop(Server,ZabState,ZabServerInfo)
		;
	#zab_req{msg=Msg}->
	    Zxid1=Msg#proposal.transaction#transaction.zxid,
	    ok=ProposalLogMod:put_proposal(Zxid1,Msg,[]),
	    Ack={Zxid1,{Mod,node()}},
	    
	    send_zab_msg({Mod,Leader},#zab_ack{msg=Ack}),
	    loop(Server#server{last_zxid=Zxid1},ZabState,ZabServerInfo);
	#zab_commit{msg=Zxid1}->
	    {ok,Proposal}=ProposalLogMod:get_proposal(Zxid1,[]),
	    Txn=Proposal#proposal.transaction,
	    {ok,_,Ns}=Mod:handle_commit(Txn#transaction.value,Zxid1,State,ZabServerInfo),
	    
	    loop(Server#server{state=Ns,last_commit_zxid=Zxid1},ZabState,ZabServerInfo);
	_ ->
	    loop(Server,ZabState,ZabServerInfo)

    end.


leading(#server{mod = Mod, state = State,
		     ensemble=Ensemble,mon_follow_refs=MRefs,
		     proposal_log_mod=ProposalLogMod,elect_pid=_EPid,
		     last_zxid=Zxid,
		quorum=Quorum,debug=_Debug,current_zxid=CurZxid,proposal_que=Que,leader=_Leader} = Server
	,Msg1,ZabState,ZabServerInfo)->
    case Msg1 of
	
	{'$proposal_call',From,Msg} ->
	    send_zab_msg(self(),{zab_proposal,From,Msg,{Mod,node()}}),
	    loop(Server,ZabState,ZabServerInfo);
	{zab_proposal,From,Msg,Sender} ->
	    {Epoch,Z}=Zxid,
	    Z1 =Z+1,
	    NewZxid={Epoch,Z1},
	    Tran=#transaction{zxid=NewZxid,value=Msg},
	    
	    Proposal=#proposal{sender=Sender,client=From,transaction=Tran},
	    ZabReq=#zab_req{msg=Proposal},
	    Acks=dict:new(),
	    
	    A2=dict:store({Mod,node()},"",Acks),
	    ets:insert(Que,#proposal_rec{zxid=NewZxid,proposal=Proposal,acks=A2}),
	    %% leader learn first
	    ProposalLogMod:put_proposal(NewZxid,Proposal,[]),
	    abcast(Mod,lists:delete(node(),Ensemble),ZabReq),
	    loop(Server#server{current_zxid=NewZxid,last_zxid=NewZxid},ZabState,ZabServerInfo)
		;
	#zab_ack{msg={Zxid1,From}}->
	    case ets:lookup(Que,Zxid1) of
		[]->
		    
		    %%maybe delay msg			    lager:info("1"),
		    do_nothing,log,
		    loop(Server,ZabState,ZabServerInfo);
		[Pro|_]->
		    Acks=Pro#proposal_rec.acks,
		    A2=dict:store(From,"",Acks),
		    Size=dict:size(A2),
		    if
			Size >=Quorum -> 
						%Result=Mod:handle_commit(Zxid,),
			    P1=Pro#proposal_rec.proposal,
			    T1=P1#proposal.transaction,
			    V1=T1#transaction.value,
			    {ok,Result,Ns}=Mod:handle_commit(V1,Zxid1,State,ZabServerInfo),
			    ZabCommit=#zab_commit{msg=Zxid1},
			    ets:delete(Que,Zxid1),
			    abcast(Mod,lists:delete(node(),Ensemble),ZabCommit),
			    reply(P1#proposal.client,Result),
			    loop(Server#server{state=Ns,last_commit_zxid=Zxid1},ZabState,ZabServerInfo)
				;
			true -> 
			    ets:insert(Que,Pro#proposal_rec{acks=A2}),
			    loop(Server,ZabState,ZabServerInfo)
		    end
	    end;
	{truncate_req,From,{Epoch1,_}}  -> 
	    {LeaderEpoch,_}=Zxid,
	    EpochLastZxid=if LeaderEpoch > Epoch1
	       ->{ok,EL}=ProposalLogMod:get_epoch_last_zxid(Epoch1,[]),EL;
	       true -> 
		    not_need
	    end,	    	    
%	    {ok,EpochLastZxid}=ProposalLogMod:get_epoch_last_zxid(Epoch1,[]),
	    M1={truncate_ack,EpochLastZxid},
	    send_zab_msg(From,M1),
	    loop(Server,ZabState,ZabServerInfo);

	{recover_req,From,StartZxid} ->
	    {ok,Res}=ProposalLogMod:iterate_zxid_count(fun({_K,V})->
				       V end,StartZxid,100),
	    
	    M1={recover_ack,Res,CurZxid},
	    
	    send_zab_msg(From,M1),
	    loop(Server,ZabState,ZabServerInfo);
	{recover_ok,From} ->	    
	    
	    NRefs=case dict:is_key(From,MRefs) of
		true->MRefs;
		false->
		   case catch erlang:monitor(process,From) of
				    {'EXIT',_}->
					MRefs;
				    Ref->
					dict:store(From,Ref,MRefs)
		   end
	    end,
	    loop(Server#server{mon_follow_refs=NRefs},ZabState,ZabServerInfo);	    
	_ ->
	    loop(Server,ZabState,ZabServerInfo)	    
    end.
abcast(Mod,Assemble,ZabReq)->
    [send_zab_msg({Mod,Node},ZabReq)||Node<-Assemble],
    ok.
    


%%-----------------------------------------------------------------
%% Callback functions for system messages handling.
%%-----------------------------------------------------------------
%% @hidden
system_continue(_Parent, _Debug, [ZabState,ZabServerInfo, Server]) ->
    loop(Server,ZabState,ZabServerInfo).


%% @hidden
system_terminate(Reason, _Parent, _Debug, [_Mode, Server, Role, E]) ->
    terminate(Reason, [], Server, Role, E).

%% @hidden
system_code_change([Mode, Server, Role, E], _Module, OldVsn, Extra) ->
    #server{mod = Mod, state = State} = Server,
    case catch Mod:code_change(OldVsn, State, E, Extra) of
        {ok, NewState} ->
            NewServer = Server#server{state = NewState},
            {ok, [Mode, NewServer, Role, E]};
        {ok, NewState, NewE} ->
            NewServer = Server#server{state = NewState},
            {ok, [Mode, NewServer, Role, NewE]};
        Else -> Else
    end.

%%-----------------------------------------------------------------
%% Format debug messages.  Print them as the call-back module sees
%% them, not as the real erlang messages.  Use trace for that.
%%-----------------------------------------------------------------
%% @hidden
print_event(Dev, {in, Msg}, Name) ->
    case Msg of
        {'$gen_call', {From, _Tag}, Call} ->
            io:format(Dev, "*DBG* ~p got local call ~p from ~w~n",
                      [Name, Call, From]);
        {'$leader_call', {From, _Tag}, Call} ->
            io:format(Dev, "*DBG* ~p got global call ~p from ~w~n",
                      [Name, Call, From]);
        {'$gen_cast', Cast} ->
            io:format(Dev, "*DBG* ~p got local cast ~p~n",
                      [Name, Cast]);
        {'$leader_cast', Cast} ->
            io:format(Dev, "*DBG* ~p got global cast ~p~n",
                      [Name, Cast]);
        _ ->
            io:format(Dev, "*DBG* ~p got ~p~n", [Name, Msg])
    end;
print_event(Dev, {out, Msg, To, State}, Name) ->
    io:format(Dev, "*DBG* ~p sent ~p to ~w, new state ~w~n",
              [Name, Msg, To, State]);
print_event(Dev, {noreply, State}, Name) ->
    io:format(Dev, "*DBG* ~p new state ~w~n", [Name, State]);
print_event(Dev, Event, Name) ->
    io:format(Dev, "*DBG* ~p dbg  ~p~n", [Name, Event]).


handle_msg({'$gen_call', From, Request} = Msg,
           #server{mod = Mod, state = State} = Server, ZabState,E) ->
    case catch Mod:handle_call(Request, From, State,E) of
        {reply, Reply, NState} ->
            NewServer = reply(From, Reply,
                              Server#server{state = NState}, ZabState, E),
            do_loop(NewServer,ZabState,E);
        {noreply, NState} = Reply ->
            NewServer = handle_debug(Server#server{state = NState},
                                     ZabState, E, Reply),
            do_loop(NewServer,ZabState,E);
        {stop, Reason, Reply, NState} ->
            {'EXIT', R} =
                (catch terminate(Reason, Msg, Server#server{state = NState},
                                 ZabState, E)),
            reply(From, Reply),
            exit(R);
        Other ->
            handle_common_reply(Other, Msg, Server,ZabState, E)
    end;
handle_msg({'$gen_cast',Msg} = Cast,
           #server{mod = Mod, state = State} = Server, Role, E) ->
    handle_common_reply(catch Mod:handle_cast(Msg, State, E),
                        Cast, Server, Role, E);


handle_msg(Msg, #server{mod = Mod, state = State} = Server, Role, E) ->
    handle_common_reply(catch Mod:handle_info(Msg, State),
                        Msg, Server, Role, E).


handle_common_reply(Reply, Msg, Server, Role, E) ->
    case Reply of
        {noreply, NState} ->
            NewServer = handle_debug(Server#server{state = NState},
                                     Role, E, Reply),
            do_loop(NewServer, Role,E);
        {ok, NState} ->
            NewServer = handle_debug(Server#server{state = NState},
                                     Role, E, Reply),
            do_loop(NewServer, Role,E);
        {stop, Reason, NState} ->
            terminate(Reason, Msg, Server#server{state = NState}, Role, E);
        {'EXIT', Reason} ->
            terminate(Reason, Msg, Server, Role, E);
        _ ->
            terminate({bad2_return_value, Reply}, Msg, Server, Role, E)
    end.


reply({To, Tag}, Reply, #server{state = State} = Server, Role, E) ->
    reply({To, Tag}, Reply),
    handle_debug(Server, Role, E, {out, Reply, To, State}).


handle_debug(#server{debug = []} = Server, _Role, _E, _Event) ->
    Server;
handle_debug(#server{debug = Debug} = Server, _Role, _E, Event) ->
    Debug1 = sys:handle_debug(Debug, {?MODULE, print_event},
                              "nouse", Event),
    Server#server{debug = Debug1}.


do_loop(Server,ZabState,ZabServerInfo) ->
    loop(Server,ZabState,ZabServerInfo).

%%% ---------------------------------------------------
%%% Terminate the server.
%%% ---------------------------------------------------

terminate(Reason, Msg, #server{mod = Mod,
                               state = State,
                               debug = Debug} = _Server, _Role,
           E) ->

    case catch Mod:terminate(Reason, State,E) of
        {'EXIT', R} ->
            error_info(R,atom_to_list(?MODULE), Msg, State, Debug),
            exit(R);
        _ ->
            case Reason of
                normal ->
                    exit(normal);
                shutdown ->
                    exit(shutdown);
                _ ->
                    error_info(Reason,atom_to_list(?MODULE), Msg, State, Debug),
                    exit(Reason)
            end
    end.

%% Maybe we shouldn't do this?  We have the crash report...
error_info(Reason, Name, Msg, State, Debug) ->
    error_logger:format("** Generic leader ~p terminating \n"
                        "** Last message in was ~p~n"
                        "** When Server state == ~p~n"
                        "** Reason for termination == ~n** ~p~n",
                        [Name, Msg, State, Reason]),
    sys:print_log(Debug),
    ok.

%%% ---------------------------------------------------
%%% Misc. functions.
%%% ---------------------------------------------------

opt(Op, [{Op, Value}|_]) ->
    {ok, Value};
opt(Op, [_|Options]) ->
    opt(Op, Options);
opt(_, []) ->
    false.

debug_options(Name, Opts) ->
    case opt(debug, Opts) of
        {ok, Options} -> dbg_options(Name, Options);
        _ -> dbg_options(Name, [])
    end.

dbg_options(Name, []) ->
    Opts =
        case init:get_argument(generic_debug) of
            error ->
                [];
            _ ->
                [log, statistics]
        end,
    dbg_opts(Name, Opts);
dbg_options(Name, Opts) ->
    dbg_opts(Name, Opts).

dbg_opts(Name, Opts) ->
    case catch sys:debug_options(Opts) of
        {'EXIT',_} ->
            error_logger:format("~p: ignoring erroneous debug options - ~p~n",
                                [Name, Opts]),
            [];
        Dbg ->
            Dbg
    end.

%%-----------------------------------------------------------------
%% Status information
%%-----------------------------------------------------------------
%% @hidden
format_status(Opt, StatusData) ->
    [PDict, SysState, Parent, Debug, [_Mode, Server, _Role, _E]] = StatusData,
    Log = sys:get_debug(log, Debug, []),
    #server{mod = Mod, state = State} = Server,
    Specific =
        case erlang:function_exported(Mod, format_status, 2) of
            true ->
                case catch apply(Mod, format_status, [Opt, [PDict, State]]) of
                    {'EXIT', _} -> [{data, [{"State", State}]}];
                    Else -> Else
                end;
            _ ->
                [{data, [{"State", State}]}]
        end,
    [{header, "Header"},
     {data, [{"Status", SysState},
             {"Parent", Parent},
             {"Logged events", Log}]} |
     Specific].


%%-----------------------------------------------------------------
%% Leader-election functions
%%-----------------------------------------------------------------

send_zab_msg(To,Msg)->
    catch erlang:send(To,#msg{cmd=?ZAB_CMD,value=Msg}).



