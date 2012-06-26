%%%-------------------------------------------------------------------
%%% @author  <>
%%% @copyright (C) 2012, 
%%% @doc
%%%
%%% @end
%%% Created : 15 Jun 2012 by  <>
%%%-------------------------------------------------------------------
-module(zabe_fast_leader_test).

-behaviour(gen_server).

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([get_state/0]).

-compile([{parse_transform, lager_transform}]).
-define(SERVER, ?MODULE). 

-record(state, {vote,state,elect_pid}).
-include("zabe_main.hrl").

%%%===================================================================
%%% API
%%%===================================================================
get_state()->
    gen_server:call(?SERVER,get).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(LastZxid,Ensemble,Quorum) ->    

    gen_server:start({local, ?SERVER}, ?MODULE, [LastZxid,Ensemble,Quorum], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([LastZxid,Ensemble,Quorum]) ->
    Election=#election{parent=?SERVER,last_zxid=LastZxid,ensemble=Ensemble,quorum=Quorum}, 
    {ok,Pid}=zabe_fast_elect:start_link(Election),
    {ok, #state{elect_pid=Pid}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(get, _From, State) ->
    Reply = State#state.state,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(#msg{value={elect_reply,{ok,V,_}}}, State) ->
    if 
	V#vote.leader =:=node()->
	    {noreply,State#state{state=?LEADING}};
	true->
	    {noreply, State#state{state=?FOLLOWING}}
    end; 
handle_info(Msg=#msg{cmd=?VOTE_CMD,value=V},State=#state{elect_pid=Pid}) ->
    lager:info("get msg ~p",[Msg]),
    gen_fsm:send_event(Pid,V),
    {noreply,State}.
      
%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
