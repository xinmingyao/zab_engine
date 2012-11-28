
%% author <yaoxinming@gmail.com>
%% implements zab protocol in erlang,detail see yahoo zab protocol paper
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
-export([send_zab_msg/3]).



-compile([{parse_transform, lager_transform}]).

-include("zabe_main.hrl").
-type option() :: {'gc_by_zab_log_count',integer()}
                | {'proposal_dir',     Dir::string()}
		| {'bucket',    non_neg_integer()} %bucket >=1 (and <2^8 or <2^40)  
                | {'memory_db',  boolean()}.

-type options() :: [option()].
-type name() :: atom().
-type server_ref() :: name() | {name(),node()} | {global,name()} | pid().
-type caller_ref() :: {pid(), reference()}.
%% Opaque state of zab_engine behaviour.

-type opts()::[{bucket,V::any()}].
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
	  mon_follow_refs::dict:new(),
	  back_end_opts::[opts()],
	  logical_clock::integer(),
	  zab_log_count::integer(),
	  gc_by_zab_log_count::integer(),
	  zab_log_count_time::integer(),
	  gc_replys::dict:new(),
	  last_gc_log_zxid::zxid(),
	  memory_db::boolean(),
	  last_snapshot_zxid::zxid(),
	  recover_msg_size::non_neg_integer()
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
     case catch gen:call(Name, '$proposal_call', Request,Timeout) of
        
        {ok,Res={error,not_ready}} ->
            Res;
	{ok,Res} ->
            Res;
        {'EXIT',Reason} ->
            exit({Reason, {?MODULE, proposal_call, [Name, Request,Timeout]}})
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
    lager:notice("#####begin start#####"),
    
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
	 {ok, State,LastCommitZxid}->
	     do_ok(Starter,CandidateNodes,LastCommitZxid,Mod,Parent,State,
		   LastCommitZxid,OptArgs,debug_options(Name, Options));
        Else ->
	     Error = {init_bad_return_value, Else},
	     proc_lib:init_ack(Starter, {error, Error}),
	     exit(Error)
    end.

do_ok(Starter,CandidateNodes,LastCommitZxid,Mod,Parent,State,LastCommitZxid,OptArgs,Debug)->
    ElectMod        = proplists:get_value(elect_mod,      OptArgs,zabe_fast_elect),
    ProposalLogMod        = proplists:get_value(proposal_log_mod,      OptArgs,zabe_proposal_leveldb_backend),
    Bucket = proplists:get_value(bucket,OptArgs,1),
    
    GcByZabLogCount = proplists:get_value(gc_by_zab_log_count,OptArgs,0),
    IsMemoryDb= proplists:get_value(is_memory_db,OptArgs,false),
    BackEndOpts=[{bucket,Bucket}],
    LastGcLogZxid=
	case catch ProposalLogMod:get_gc(BackEndOpts) of
	     {ok,_,T1}->
		     T1;
		  _->
		     {0,0}
	     end,
    RecoverMsgSize=proplists:get_value(recover_msg_size,OptArgs,100),
    proc_lib:init_ack(Starter, {ok, self()}),
    LastSnapshotZxid= case IsMemoryDb of
			  true->
			      LastCommitZxid;
			  false->
			      {0,0}
		      end,
    Ensemble=CandidateNodes,
	     Quorum=ordsets:size(Ensemble) div  2  +1,
	     LastZxid= case ProposalLogMod:get_last_proposal(BackEndOpts) of
			   {ok,not_found}->{0,0};
			   {ok,Z}->Z
		       end,
	     LogicalClock=1,
	     Election=#election{logical_clock=LogicalClock,parent=Mod,last_zxid=LastZxid,ensemble=Ensemble,quorum=Quorum,last_commit_zxid=LastCommitZxid},
	     
	     {ok,Pid}=ElectMod:start_link(Election),
	     Que=ets:new(list_to_atom(atom_to_list(Mod)++"_que"),[{keypos,2},ordered_set]),
	     loop(#server{parent = Parent,mod = Mod,elect_pid=Pid,
				  ensemble=Ensemble,
				  last_commit_zxid=LastCommitZxid,
			          recover_acks=dict:new(),
			  recover_msg_size=RecoverMsgSize,
			  mon_follow_refs=dict:new(),
			  back_end_opts=BackEndOpts,
				  state = State,last_zxid=LastZxid,current_zxid=LastZxid,proposal_log_mod=ProposalLogMod,
			  logical_clock=LogicalClock,
			  zab_log_count=0,
			  gc_replys=dict:new(),
			  gc_by_zab_log_count=GcByZabLogCount,
			  last_gc_log_zxid=LastGcLogZxid,    
			  memory_db=IsMemoryDb,
			  last_snapshot_zxid=LastSnapshotZxid,
				  debug = Debug,quorum=Quorum,proposal_que=Que},looking,#zab_server_info{}
			 ).
%%% ---------------------------------------------------
%%% The MAIN loops.
%%% ---------------------------------------------------

loop(Server=#server{debug=Debug,elect_pid=EPid,last_zxid=LastZxid,
		    last_commit_zxid=LastCommitZxid,proposal_que=Que,
		    mon_follow_refs=MonFollowRefs,quorum=Quorum,
		    leader=Leader,
		    mod=Mod,ensemble=Ensemble,
		    gc_replys=GcReplys,
		    back_end_opts=BackEndOpts,
		    proposal_log_mod=ProposalLogMod,
		    state=State,
		    mon_leader_ref=_ModLeaderRef},ZabState,ZabServerInfo)->
    receive
	Msg1->
	    lager:debug("zab state:~p receive msg:~p",[ZabState,Msg1]),
	    case Msg1 of
		{zab_system,snapshot}->
		    case Server#server.memory_db of
			true->
			    case Mod:do_snapshot(LastZxid,State) of
				{ok,N1}->
				    loop(Server#server{last_snapshot_zxid=LastZxid,state=N1},ZabState,ZabServerInfo);
				_->
				    lager:error("~p :zab snapshot error",[]),
				    loop(Server,ZabState,ZabServerInfo)
			    end;
			false->
			    loop(Server,ZabState,ZabServerInfo)
		    end;
		{zab_system,gc}->
		    GcReq=#gc_req{from=node()},
		    abcast(Mod,lists:delete(node(),Ensemble),GcReq,Server#server.logical_clock),
		    loop(Server,ZabState,ZabServerInfo);
		{zab_system,From,zab_info}->
		    Ret=[{last_gc_log_zxid,Server#server.last_gc_log_zxid}
			 ,{last_commit_zxid,Server#server.last_commit_zxid}
			 ,{last_zxid,Server#server.last_zxid}
			 ,{zab_log_count,Server#server.zab_log_count}
			 ,{zab_log_count_time,Server#server.zab_log_count}
			 ,{gc_by_zab_log_count,Server#server.gc_by_zab_log_count}
			 ,{msg_que_size,ets:info(Server#server.proposal_que,size)}
			 ,{is_leader,Leader=:=node()}
			 ,{ensemble,Ensemble}],
		    catch erlang:send(From,{zab_info,Ret}),
		    loop(Server,ZabState,ZabServerInfo);
		{'DOWN',_ModLeaderRef,_,{_,Leader},_}->
		    ets:delete_all_objects(Que),
		    gen_fsm:send_event(EPid,{re_elect,LastZxid,LastCommitZxid}),
		    lager:notice("##leader:~p down zxid:~p,commit zxid:~p,change state~p to looking##",[Leader,LastZxid,LastCommitZxid,ZabState]),
		    loop(Server#server{
			    recover_acks=dict:new(),
			    mon_follow_refs=dict:new(),
			   gc_replys=dict:new(),
			   logical_clock=Server#server.logical_clock+1
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
			    lager:notice("##lose quorum:~p,zxid:~p,commit zxid:~p,change state~p to looking##",
					 [dict:to_list(N1),LastZxid,LastCommitZxid,ZabState]),
			    gen_fsm:send_event(EPid,{re_elect,LastZxid,LastCommitZxid}),
			    M1={partition,less_quorum},
			    abcast(Mod,lists:delete(node(),Ensemble),M1,Server#server.logical_clock),
			    loop(Server#server{
				    recover_acks=dict:new(),
				    mon_follow_refs=dict:new(),
				   gc_replys=dict:new(),
				   logical_clock=Server#server.logical_clock+1
				   },looking,#zab_server_info{}
				)
		    end
			     
		    ;
		    
		#msg{cmd=?VOTE_CMD,value=V} ->
		    gen_fsm:send_event(EPid,V),
		    loop(Server,ZabState,ZabServerInfo);
		#msg{epoch=Epoch} when Epoch < Server#server.logical_clock ->
		    %fush invalidate msg
		    loop(Server,ZabState,ZabServerInfo);
		#msg{value=Rep=#gc_reply{from=ReplyFrom}}->
		    G2=dict:store(ReplyFrom,Rep,GcReplys),
		    T1=dict:size(G2),
		    LogLast=case Server#server.memory_db of
				true->Server#server.last_snapshot_zxid;
				false->Server#server.last_commit_zxid end,
		    Temp=
			if T1=:=length(Ensemble)-1 ->
				LastCommit=dict:fold(fun(_K,#gc_reply{last_commit_log_zxid=Zxid},Acc)->
							 T=zabe_util:zxid_big(Zxid,Acc),
							     if 
								 T->
								     Acc;
								 true->Zxid
							     end end
						     ,LogLast,G2),
				
				case LastCommit of
				    {0,0}->Server#server.last_gc_log_zxid;
				    _->
					ProposalLogMod:gc(LastCommit,BackEndOpts),
					LastCommit
				end;
			   true->Server#server.last_gc_log_zxid
			end,
		    loop(Server#server{last_gc_log_zxid=Temp,gc_replys=G2},ZabState,ZabServerInfo);
		#msg{value=#gc_req{from=GcFrom}}->
		    MyLastLogCommit=case Server#server.memory_db of
					true->Server#server.last_snapshot_zxid;
					false->
					    Server#server.last_commit_zxid end,
		    GcRep=#gc_reply{from=node(),last_commit_log_zxid=MyLastLogCommit,time=get_timestamp()},
		    send_zab_msg({Mod,GcFrom},GcRep,Server#server.logical_clock),
		    loop(Server,ZabState,ZabServerInfo);
		%leader partiton 
		#msg{value={partition,less_quorum}}-> 
		    ets:delete_all_objects(Que),
		    
		    gen_fsm:send_event(EPid,{re_elect,LastZxid,LastCommitZxid}),
		    lager:notice("##begin re-elect ,zxid:~p,commit zxid:~p,change state~p to looking##",[LastZxid,LastCommitZxid,ZabState]),
		    loop(Server#server{
			    recover_acks=dict:new(),
			    mon_follow_refs=dict:new(),
			   gc_replys=dict:new(),
			   logical_clock=Server#server.logical_clock+1
			   },looking,#zab_server_info{}
			 );

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
		ensemble=Ensemble,
		proposal_log_mod=ProposalLogMod,back_end_opts=BackEndOpts} = Server
	,Msg1,ZabState,ZabServerInfo)->
    case Msg1 of
	{elect_reply,{ok,V=#vote{leader=Node},RecvVotes}} when Node=:=node() ->	    
	    erlang:put(vote,V),
	    Last=case zabe_util:zxid_eq(LastZxid,LastCommitZxid) of
		true -> LastCommitZxid;
		false->{ok,{T,_}}=ProposalLogMod:fold(fun({_Key,P1},{_LastCommit,Count})->
					   
					   Mod:handle_commit(P1#p.transaction#t.value,
							     P1#p.transaction#t.zxid,State,ZabServerInfo) ,
						   {P1#p.transaction#t.zxid,Count}
					   end,
						 {LastCommitZxid,infinite},LastCommitZxid,BackEndOpts),
		       T
	    end,
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
		    loop(Server#server{role=?LEADING,leader=Node,last_zxid=Last,mon_follow_refs=N5,
				       last_commit_zxid=Last},leader_recover,ZabServerInfo) ;
	       true-> 
		    ets:delete_all_objects(Que),
		    gen_fsm:send_event(EPid,{re_elect,LastZxid,LastCommitZxid}),
		    M1={partition,less_quorum},
		    abcast(Mod,lists:delete(node(),Ensemble),M1,Server#server.logical_clock),
			    loop(Server#server{
				    recover_acks=dict:new(),
				    mon_follow_refs=dict:new(),
				   logical_clock=Server#server.logical_clock+1
				   },looking,#zab_server_info{}
				)
	    end;
	{elect_reply,{ok,V=#vote{leader=Node,zxid=_LeaderZxid},_}}  -> 
	    erlang:put(vote,V),
	    %monitor_leader(Mod,Node),
	    case catch erlang:monitor(process,{Mod,Node}) of
		{'EXIT',_}->
		    ets:delete_all_objects(Que),
		    gen_fsm:send_event(EPid,{re_elect,LastZxid,LastCommitZxid}),
		    lager:notice("##leader down re-elect,zxid:~p,commit zxid:~p,change state~p to looking",[LastZxid,LastCommitZxid,ZabState]),
		    loop(Server#server{
			    recover_acks=dict:new(),
			    mon_follow_refs=dict:new(),
			    logical_clock=Server#server.logical_clock+1
			   },looking,#zab_server_info{}
			 );
		Ref->
		    timer:sleep(50),%%todo fix this by leader handle this
		    M1={truncate_req,{Mod,node()},LastZxid},
		    
		    send_zab_msg({Mod,Node},M1,V#vote.epoch),
		    loop(Server#server{leader=Node,logical_clock=V#vote.epoch,role=?FOLLOWING,mon_leader_ref=Ref},follow_recover,ZabServerInfo) 
	    end;
	 _ -> %flush msg
	    loop(Server,ZabState,ZabServerInfo) 
    end
.


leader_recover(#server{mod = _Mod, state = _State,debug=_Debug,quorum=Quorum,elect_pid=_EPid,
		last_zxid=LastZxid,last_commit_zxid=_LastCommitZxid,recover_acks=RecoverAcks,
		proposal_log_mod=ProposalLogMod,
		       back_end_opts=BackEndOpts,
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
		    lager:notice("##leader recover change state to leading##"),
		    %%change epoch for handle old leader restart
		    Z1=zabe_util:change_leader_zxid(LastZxid),
		    loop(Server#server{recover_acks=D1,mon_follow_refs=NRefs,last_zxid=Z1,last_commit_zxid=Z1},leading,ZabServerInfo);
	       true->
		    loop(Server#server{recover_acks=D1,mon_follow_refs=NRefs},ZabState,ZabServerInfo)
	    end;
	{truncate_req,From,{Epoch1,_}}  -> 
	    {LeaderEpoch,_}=LastZxid,
	    EpochLastZxid=if LeaderEpoch > Epoch1
	       ->{ok,EL}=ProposalLogMod:get_epoch_last_zxid(Epoch1,BackEndOpts),EL;
	       true -> 
		    not_need
	    end,	    	    
	    M1={truncate_ack,EpochLastZxid},
	    send_zab_msg(From,M1,Server#server.logical_clock),
	    loop(Server,ZabState,ZabServerInfo);

	{recover_req,From,StartZxid} ->
	    case zabe_util:zxid_big(Server#server.last_gc_log_zxid,StartZxid) of
		false->
		    {ok,{Res,_}}=
			ProposalLogMod:fold(fun({_Key,Value},{Acc,Count})->		
						    {[Value|Acc],Count-1} end,{[],Server#server.recover_msg_size},StartZxid,BackEndOpts),
    
		    M1={recover_ack,lists:reverse(Res),CurZxid},
		    send_zab_msg(From,M1,Server#server.logical_clock);
		true-> %%gc is big peer last_zxid can not recover by log
		    M2={last_gc_big},
		    send_zab_msg(From,M2,Server#server.logical_clock)
	    end,		
	    loop(Server,ZabState,ZabServerInfo);
	_ -> %flush msg
	    loop(Server,ZabState,ZabServerInfo) 
    end
.

follow_recover(#server{mod =Mod, state = State,quorum=_Quorum,leader=Leader,
		last_zxid=LastZxid,last_commit_zxid=LastCommitZxid,recover_acks=_RecoverAcks,
		proposal_que=Que,
		       
		       back_end_opts=BackEndOpts,
		proposal_log_mod=ProposalLogMod} = Server
	,Msg1,ZabState,ZabServerInfo)->
    case Msg1 of
	{recover_ack,Proposals,_LeaderCurrentZxid} ->
	    Last=lists:foldl(fun(Proposal,_)->
			      ProposalLogMod:put_proposal(Proposal#p.transaction#t.zxid,Proposal,BackEndOpts),
			      Mod:handle_commit(Proposal#p.transaction#t.value,
						Proposal#p.transaction#t.zxid,State,ZabServerInfo),
				Proposal#p.transaction#t.zxid
		      end ,LastZxid,Proposals),
	    QueSize=ets:info(Que,size),
	   
	    case Proposals of
		[] when QueSize=:=0->
		    M1={recover_ok,{Mod,node()}},
		    send_zab_msg({Mod,Leader},M1,Server#server.logical_clock),
		    lager:notice("##follow recover ok,state to followiing##"),
		    loop(Server#server{last_zxid=Last,last_commit_zxid=Last},following,ZabServerInfo);
		[] when QueSize>0-> %recover local
		    
		    F=fun(P1)->
			      Pp=P1#proposal_rec.proposal,
			      Z1=Pp#p.transaction#t.zxid,
			      ProposalLogMod:put_proposal(Z1,Pp,BackEndOpts),
			      Mod:handle_commit(Pp#p.transaction#t.value,Z1,State,ZabServerInfo),
							 Z1
						  end,
		    NewZxid=fold_all(Que,F,Last,ets:first(Que),LastZxid),
		    lager:notice("##follow recover local ok,state to followiing##"),
		    loop(Server#server{last_zxid=NewZxid,last_commit_zxid=NewZxid},following,ZabServerInfo)    
			;

		_ when QueSize=:=0->
		    M1={recover_req,{Mod,node()},zabe_util:zxid_plus(Last)},
			    send_zab_msg({Mod,Leader},M1,Server#server.logical_clock),
		    loop(Server#server{last_zxid=Last,last_commit_zxid=Last},ZabState,ZabServerInfo);
		_ when QueSize>0 ->
		    Min=ets:first(Que),
                    case zabe_util:zxid_big_eq(Last,Min) of
			true->		
			
			   % lager:debug(" recover local ~p ~p",[Last,MinZxid]),
			    F=fun(P1)->
				      Pp=P1#proposal_rec.proposal,
				      Z1=Pp#p.transaction#t.zxid,
				      ProposalLogMod:put_proposal(Z1,Pp,BackEndOpts),
				      Mod:handle_commit(Pp#p.transaction#t.value,Z1,State,ZabServerInfo),
							 Z1
						  end,
			   % NewZxid=fold_all(Que,F,Last),
			    NewZxid=fold_all(Que,F,Last,Min,Last),
			    %ets:delete_all_objects(Que),
			    lager:notice("##follow recover ok,state to followiing##"),
			    loop(Server#server{last_zxid=NewZxid,last_commit_zxid=NewZxid},following,ZabServerInfo)    
				;
			false ->
			    
			    M1={recover_req,{Mod,node()},zabe_util:zxid_plus(Last)},
			    send_zab_msg({Mod,Leader},M1,Server#server.logical_clock),
			    loop(Server#server{last_zxid=Last,last_commit_zxid=Last},ZabState,ZabServerInfo)
		    end
	    end
		;
	{truncate_ack,LeaderEpochLastZxid}  ->
	    MZxid=case LeaderEpochLastZxid of
		      not_need->LastZxid;
		      not_found->LastZxid;
		      _->
			  
			  {ok,{L2,_}}=
			      ProposalLogMod:fold(fun({Key,_Value},{Acc,Count})->		
							  {[Key|Acc],Count} end,{[],infinite},
						  zabe_util:zxid_plus(LeaderEpochLastZxid),BackEndOpts),
			
			  lists:map(fun(Key)->
					    ProposalLogMod:delete_proposal(Key,BackEndOpts) end,lists:reverse(L2)),
			  LeaderEpochLastZxid
	    end,
            %%%  recover local,redo local log

	    {ok,{_T,_}}=ProposalLogMod:fold(fun({_Key,P1},{_LastCommit,Count})->
						   
						   Mod:handle_commit(P1#p.transaction#t.value,
								     P1#p.transaction#t.zxid,State,ZabServerInfo) ,
						   {P1#p.transaction#t.zxid,Count}
					   end,
					   {LastCommitZxid,infinite},LastCommitZxid,BackEndOpts),
	    
    	    
            %%%
       
	    M1={recover_req,{Mod,node()},zabe_util:zxid_plus(MZxid)},
	    send_zab_msg({Mod,Leader},M1,Server#server.logical_clock),
	    loop(Server#server{last_zxid=MZxid,last_commit_zxid=MZxid},ZabState,ZabServerInfo);
	#zab_req{msg=Msg}->
	    Zxid1=Msg#p.transaction#t.zxid,
	    ets:insert(Que,#proposal_rec{zxid=Zxid1,proposal=Msg,commit=false}),
	    loop(Server,ZabState,ZabServerInfo);
	#zab_commit{msg=Zxid1}->
	    case  ets:lookup(Que,Zxid1) of
		[P1|_]->
		    ets:insert(Que,P1#proposal_rec{commit=true});
		[]-> %maybe handle_commit on recover
		    do_nothing
	    end,		    
	    loop(Server,ZabState,ZabServerInfo);
	{last_gc_big}->
	    lager:critical("leader last gc zxid >peer last_zxid ,can not recover,node will halt()"),
	    %TODO use file transfer,copy log file from peer
	    erlang:halt("leader last gc zxid >peer last_zxid ,can not recover,node will halt()");
	_ -> %flush msg %todo save proposal msg into que
	    loop(Server,ZabState,ZabServerInfo) 
    end
.
			  
fold_all(Que,F,Last,Key,Acc)->
 case ets:lookup(Que,Key) of
     [P1|_]->
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



following(#server{mod = Mod, state = State,
		  ensemble=Ensemble,
		  back_end_opts=BackEndOpts,
		  proposal_log_mod=ProposalLogMod,elect_pid=_EPid,
		  zab_log_count=ZabLogCount,
		  gc_by_zab_log_count=GcByZabLogCount,
		  zab_log_count_time=ZabLogCountTime,
		  quorum=_Quorum,debug=_Debug,last_zxid=_Zxid,proposal_que=_Que,leader=Leader} = Server,Msg1,ZabState,ZabServerInfo)->

    case Msg1 of
	{'$proposal_call',From,Msg} ->
	    send_zab_msg({Mod,Leader},{zab_proposal,From,Msg,node()},Server#server.logical_clock),
	    loop(Server,ZabState,ZabServerInfo)
		;
	#zab_req{msg=Msg}->
	    Zxid1=Msg#p.transaction#t.zxid,
	    ok=ProposalLogMod:put_proposal(Zxid1,Msg,BackEndOpts),
	    NTime=if ZabLogCount=:=0
		-> get_timestamp();
		true ->
			  ZabLogCountTime
	    end,
	    NCount=ZabLogCount+1,
	    N2Count=if GcByZabLogCount =/= 0 andalso NCount>GcByZabLogCount
		       ->
			    GcReq=#gc_req{from=node()},
			    abcast(Mod,lists:delete(node(),Ensemble),GcReq,Server#server.logical_clock),
				0;
		       true ->NCount 
		    end,
	    Ack={Zxid1,{Mod,node()}},

	    send_zab_msg({Mod,Leader},#zab_ack{msg=Ack},Server#server.logical_clock),
	    loop(Server#server{last_zxid=Zxid1,zab_log_count=N2Count,zab_log_count_time=NTime},ZabState,ZabServerInfo);
	#zab_commit{msg=Zxid1}->
	     case ProposalLogMod:get_proposal(Zxid1,BackEndOpts) of
		 {ok,Proposal}-> 
		     Txn=Proposal#p.transaction,
		     {ok,Result,Ns}=Mod:handle_commit(Txn#t.value,Zxid1,State,ZabServerInfo),
		     PeerNode=Proposal#p.sender,
			    if PeerNode =:=node() ->
				    reply(Proposal#p.client,Result);
			       true->
				    do_nothing
			    end,
		     loop(Server#server{state=Ns,last_commit_zxid=Zxid1},ZabState,ZabServerInfo);
		 _-> %maybe commit in recover,ignore
		     loop(Server,ZabState,ZabServerInfo)
	     end;
	    
	_ ->
	    loop(Server,ZabState,ZabServerInfo)

    end.


leading(#server{mod = Mod, state = State,
		     ensemble=Ensemble,mon_follow_refs=MRefs,
		     proposal_log_mod=ProposalLogMod,elect_pid=_EPid,
		     last_zxid=Zxid,
		zab_log_count=ZabLogCount,
		gc_by_zab_log_count=GcByZabLogCount,
		zab_log_count_time=ZabLogCountTime,
		back_end_opts=BackEndOpts,
		quorum=Quorum,debug=_Debug,current_zxid=CurZxid,proposal_que=Que,leader=_Leader} = Server
	,Msg1,ZabState,ZabServerInfo)->
    case Msg1 of
	
	{'$proposal_call',From,Msg} ->
	    send_zab_msg(self(),{zab_proposal,From,Msg,node()},Server#server.logical_clock),
	    loop(Server,ZabState,ZabServerInfo);
	{zab_proposal,From,Msg,Sender} ->
	    {Epoch,Z}=Zxid,
	    Z1 =Z+1,
	    NewZxid={Epoch,Z1},
	    Tran=#t{zxid=NewZxid,value=Msg},
	    
	    Proposal=#p{sender=Sender,client=From,transaction=Tran},
	    ZabReq=#zab_req{msg=Proposal},
	    Acks=dict:new(),
	    
	    A2=dict:store({Mod,node()},"",Acks),
	    ets:insert(Que,#proposal_rec{zxid=NewZxid,proposal=Proposal,acks=A2}),
	    %% leader learn first
	    ProposalLogMod:put_proposal(NewZxid,Proposal,BackEndOpts),
	    %%
	    NTime=if ZabLogCount=:=0
		-> get_timestamp();
		true ->
			  ZabLogCountTime
	    end,
	    NCount=ZabLogCount+1,
	    N2Count=if GcByZabLogCount =/= 0 andalso NCount>GcByZabLogCount
		       ->
			    GcReq=#gc_req{from=node()},
			    abcast(Mod,lists:delete(node(),Ensemble),GcReq,Server#server.logical_clock),
				0;
		       true ->NCount 
		    end,
	    %%
	    abcast2(Mod,lists:delete(node(),Ensemble),ZabReq,Server#server.logical_clock),
	    loop(Server#server{zab_log_count_time=NTime,zab_log_count=N2Count,current_zxid=NewZxid,last_zxid=NewZxid},ZabState,ZabServerInfo)
		;
	#zab_ack{msg={Zxid1,From}}->
	    case ets:lookup(Que,Zxid1) of
		[]->
		    
		    %%maybe delay msg			    lager:debug("1"),
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
			    T1=P1#p.transaction,
			    V1=T1#t.value,
			    {ok,Result,Ns}=Mod:handle_commit(V1,Zxid1,State,ZabServerInfo),
			    ZabCommit=#zab_commit{msg=Zxid1},
			    ets:delete(Que,Zxid1),
			    abcast2(Mod,lists:delete(node(),Ensemble),ZabCommit,Server#server.logical_clock),
			    PeerNode=P1#p.sender,
			    if PeerNode =:=node() ->
				    reply(P1#p.client,Result);
			       true->
				    do_nothing
			    end,
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
	       ->{ok,EL}=ProposalLogMod:get_epoch_last_zxid(Epoch1,BackEndOpts),EL;
	       true -> 
		    not_need
	    end,	    	    
%	    {ok,EpochLastZxid}=ProposalLogMod:get_epoch_last_zxid(Epoch1,[]),
	    M1={truncate_ack,EpochLastZxid},
	    send_zab_msg(From,M1,Server#server.logical_clock),
	    loop(Server,ZabState,ZabServerInfo);

	{recover_req,From,StartZxid} ->
	    case zabe_util:zxid_big(Server#server.last_gc_log_zxid,StartZxid) of
		false->
		    {ok,{Res,_}}=
			ProposalLogMod:fold(fun({_Key,Value},{Acc,Count})->		
						    {[Value|Acc],Count-1} end,{[],Server#server.recover_msg_size},StartZxid,BackEndOpts),
    
		    M1={recover_ack,lists:reverse(Res),CurZxid},
		    send_zab_msg(From,M1,Server#server.logical_clock);
		true-> %%gc is big peer last_zxid can not recover by log
		    M2={last_gc_big},
		    send_zab_msg(From,M2,Server#server.logical_clock)
	    end,		
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
abcast(Mod,Assemble,ZabReq,LC)->
    [send_zab_msg({Mod,Node},ZabReq,LC)||Node<-Assemble],
    ok.
%%do not send msg to offline node,which will cause epmd high cpu
abcast2(Mod,Assemble,ZabReq,LC)->
    Ns=nodes(),
    N=lists:filter(fun(A)->
			   is_elem(A,Ns) end,Assemble),
    abcast(Mod,N,ZabReq,LC),
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
    handle_common_reply(catch Mod:handle_info(Msg, State,E),
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

send_zab_msg(To,Msg,LC)->
    catch erlang:send(To,#msg{cmd=?ZAB_CMD,value=Msg,epoch=LC}).


get_timestamp()->
    calendar:datetime_to_gregorian_seconds(erlang:localtime()).

is_elem(_A,[])->
    false;
is_elem(H,[H|T]) ->
    true;
is_elem(A,[_|T]) ->
    is_elem(A,T).
