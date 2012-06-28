%%%-------------------------------------------------------------------
%%% @author  <>
%%% @copyright (C) 2012, 
%%% @doc
%%%
%%% @end
%%% Created : 14 Jun 2012 by  <>
%%%-------------------------------------------------------------------
-module(zabe_fast_elect).

-behaviour(gen_fsm).
%% API
-export([start_link/1]).

%% gen_fsm callbacks
-export([init/1, looking/2, state_name/3, handle_event/3,
	 handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).
 

-export([wait_outof_election/2]).
-compile([{parse_transform, lager_transform}]).
 
-include("zabe_main.hrl").
-define(SERVER, ?MODULE).


-record(state, {cur_vote::#vote{},
		manager_name::atom(),
		ensemble,
		quorum,
		ntimeout,
		recv_votes::ets,
		outof_election::ets,
		wait_outof_timer::reference(),
		vote_timer::reference()
}).


-define(WAIT_TIMEOUT,500).
-define(PROPOSED,proposed).



%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% fast leader select for zab,algorithem from zookeeper,FasterLeaderElection.java
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
-spec start_link(Election::#election{})->
			{ok,Pid::pid()}|{error,Reason::any()}.
start_link(Election) ->
    gen_fsm:start(?MODULE, [Election], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([#election{parent=ManagerName,last_zxid=LastZxid,ensemble=Ensemble,quorum=Quorum,last_commit_zxid=LastCommitZxid}]) ->
    lager:info("elect start"),
   % {Epoch,_TxnId}=LastZxid,
    LogicalLock=1,
    V=#vote{from=node(),leader=node(),zxid=LastZxid,
	    last_commit_zxid=LastCommitZxid,
	    epoch=LogicalLock,state=?LOOKING},
    
    send_notifications(V,Ensemble,ManagerName),
    {ok,TimeRef}=timer:apply_interval(3000,?MODULE,send_notifications,[V,Ensemble,ManagerName]),
    Recv=ets:new(list_to_atom(atom_to_list(ManagerName)++"_1"),[{keypos,2}]),
    OutOf=ets:new(list_to_atom(atom_to_list(ManagerName)++"_2"),[{keypos,2}]),
    put(?PROPOSED,V),
    {ok, looking, #state{
	   manager_name=ManagerName,
	   ensemble=Ensemble,
	   quorum=Quorum,
	   recv_votes=Recv,
	   outof_election=OutOf,
	   vote_timer=TimeRef
	  }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% 
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
looking(V=#vote{},State)->
    lager:info("receive vote:~p ~n",[V]),
    try looking1(V,State) of
	_->
	    {next_state, looking, State}
    catch
	throw:break ->
	    {next_state, looking, State};
	throw:{wait_outof,Ns}->
	    {next_state, wait_outof_election, Ns};
	throw:finish->
	    {stop,normal, State}
    end
;		      

looking(_Event, State) ->
   {next_state, looking, State}.


looking1(Vote=#vote{from=_From,leader=Leader,state=PeerState,epoch=PeerEpoch}, State=#state{manager_name=ManagerName,ensemble=Ensemble,vote_timer=VoteTimer}) ->

    P1=get(?PROPOSED),
    Epoch=P1#vote.epoch,  
    case 
	PeerState of
	?LOOKING->
	    if
		PeerEpoch < Epoch->
		    send_notifications(P1,Ensemble,ManagerName),
		    throw(break);
		PeerEpoch >Epoch->
		    case 
			total_order_predicate(Vote#vote.leader,Vote#vote.zxid,P1#vote.leader,P1#vote.zxid) of
			true->
			    V1=P1#vote{leader=Leader,zxid=Vote#vote.zxid,epoch=PeerEpoch},
			    send_notifications(V1,Ensemble,ManagerName),
			    ets:delete_all_objects(State#state.recv_votes),
			    put(?PROPOSED,V1);
			false->
			    V1=P1#vote{epoch=PeerEpoch},
			    send_notifications(V1,Ensemble,ManagerName),
			    ets:delete_all_objects(State#state.recv_votes),
			    put(?PROPOSED,V1)
		    end;
		true -> 
		    case 
			total_order_predicate(Vote#vote.leader,Vote#vote.zxid,P1#vote.leader,P1#vote.zxid) of
		    true->
			    V1=P1#vote{leader=Vote#vote.leader,zxid=Vote#vote.zxid,epoch=PeerEpoch},

			    send_notifications(V1,Ensemble,ManagerName),
			    
			    put(?PROPOSED,V1);
			false->
			    ok
			    
		    end
			
	    end,
	    RecvVote=State#state.recv_votes,
	    ets:insert(RecvVote,Vote),
	    
	    ReceiveAll=ets:info(RecvVote,size) == ordsets:size(Ensemble),
	    HaveQuorm=is_have_quorm(State#state.quorum,Vote,RecvVote),
	    if ReceiveAll ->
		    timer:cancel(VoteTimer),
		    notify_manager(ManagerName,ets:tab2list(RecvVote)),
		    throw(finish);
	       HaveQuorm->
		    %wait after select
		    TimeRef=gen_fsm:start_timer(?WAIT_TIMEOUT,wait_timeout),
		    
		    throw({wait_outof,State#state{wait_outof_timer=TimeRef}})
		    ;
	       true ->
		    ok
	    end;
	?OBSERVING->
	    ok;
	?LEADING->
	    if PeerEpoch =:=Epoch->
		    R2=State#state.recv_votes,
		    HaveQuorm=is_have_quorm(State#state.quorum,Vote,R2),
		    CheckLeader=check_leader(R2,Vote#vote.leader,node()),
		    if Vote#vote.state ==?LEADING andalso CheckLeader andalso HaveQuorm->
			    timer:cancel(VoteTimer),
			    notify_manager(ManagerName,ets:tab2list(State#state.recv_votes)),
			    throw(finish);
		       true ->
			    ok
		    end;
	       true ->
		    OutOf=State#state.outof_election,
		    O2=ets:insert(OutOf,Vote),
		    HaveQuorm=is_have_quorm(State#state.quorum,Vote,O2),
		    CheckLeader=check_leader(O2,Vote#vote.leader,node()),
		    if HaveQuorm andalso CheckLeader->
			    V1=P1#vote{epoch=PeerEpoch},
			    put(?PROPOSED,V1),
			    timer:cancel(VoteTimer),
			    notify_manager(ManagerName,ets:tab2list(State#state.recv_votes)),
			    ets:delete_all_objects(State#state.recv_votes),%??
			    throw(finish)
			    ;
		       
		       true ->
			    ok
		    end
	    end
    end
    .

wait_outof_election({timeout,_,wait_timeout},State=#state{manager_name=M,vote_timer=VoteTimer})->
    timer:cancel(VoteTimer),
    notify_manager(M,ets:tab2list(State#state.recv_votes)),
   {stop,normal, State};
wait_outof_election(Vote=#vote{leader=_Leader,state=?LOOKING},State) ->
   % P1=get(?PROPOSED),
   % case total_order_predicate(Leader,Vote#vote.zxid,node(),P1#vote.zxid) of
   %	true->
	    %send to myself
	    gen_fsm:cancel_timer(State#state.wait_outof_timer),
	    gen_fsm:send_event_after(1,Vote),
	    {next_state, looking, State}
%	_->
	    %TimeRef=gen_fsm:start_timer(?WAIT_TIMEOUT,wait_timeout),
%	    {next_state,wait_outof_election, State}
%    end.
.
is_have_quorm(Q,Proposed,RecvVote)->
    C1=ets:foldl(
      fun(Vote,Count)->
		 Eq=is_eq(Vote,Proposed),
		 if
		     Eq-> Count+1;
		     true -> Count  
		 end end ,0,RecvVote),
    if C1 >=Q->
	    true;
       true ->
	    false
    end.
is_eq(V1,V2)->
    V1#vote.leader=:=V2#vote.leader andalso V1#vote.zxid=:=V2#vote.zxid andalso V1#vote.epoch=:=V2#vote.epoch.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
state_name(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, state_name, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

send_notifications(V=#vote{},Ensemble,ManagerName)->
    Msg=#msg{cmd=?VOTE_CMD,value=V},
    lists:map(fun(N)->
		      lager:info("send ~p ~p",[N,ManagerName]),
		      erlang:send({ManagerName,N},Msg) end ,Ensemble).
    
%    R=[erlang:send({ManagerName,Node},Msg)||Node<-Ensemble],
%    lager:info("send ~p",[R]) 
	
				     
total_order_predicate(New,NewZxid,Cur,CurZxid)->
    (NewZxid>CurZxid) orelse ((NewZxid==CurZxid) andalso (New>Cur)).
notify_manager(ManagerName,RecvVotes) ->
    V=get(?PROPOSED),
    lager:info("elect1 ~p~n",[V]),
    erlang:send(ManagerName,#msg{cmd=?ZAB_CMD,value={elect_reply,{ok,V,RecvVotes}}}).

check_leader(_Votes,Leader,Leader) ->
    true;
check_leader(Votes,Leader,_) ->
    case ets:lookup(Votes,Leader) of
	[]->
	    false;
	_ ->
	    true
    end.
		
