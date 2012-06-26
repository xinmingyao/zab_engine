%%%-------------------------------------------------------------------
%%% @author  <>
%%% @copyright (C) 2012, 
%%% @doc
%%%
%%% @end
%%% Created : 19 Jun 2012 by  <>
%%%-------------------------------------------------------------------
-module(zabe_proposal_leveldb_backend).
-behaviour(gen_server).
-behaviour(zabe_proposal_backend).
%% API
-export([start_link/1,start/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 
-include("zabe_main.hrl").
-compile([{parse_transform, lager_transform}]).

-record(state, {leveldb}).
-define(MAX_PROPOSAL,999999999999999999999999999999).


-export([put_proposal/3,get_proposal/2,get_last_proposal/1,fold/3,get_epoch_last_zxid/2]).


%%%===================================================================
%%% API
%%%===================================================================
get_last_proposal(Opts)->
    gen_server:call(?SERVER,{get_last_proposal,Opts}) .
put_proposal(Key,Proposal,Opts)->
    gen_server:call(?SERVER,{put_proposal,Key,Proposal,Opts})   
.
get_proposal(Key,Opts)->
    gen_server:call(?SERVER,{get_proposal,Key,Opts})   
	.
fold(Fun,Start,Opts)->
    gen_server:call(?SERVER,{fold,Fun,Start,Opts}).
get_epoch_last_zxid(Epoch,Opts)->
    gen_server:call(?SERVER,{get_epoch_last_zxid,Epoch,Opts}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start(Dir)->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Dir], []).
start_link(Dir) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Dir], []).

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
init([Dir]) ->    
   % WorkDir="/home/erlang/tmp/proposal.bb",
    case eleveldb:open(Dir, [{create_if_missing, true},{max_open_files,50}]) of
        {ok, Ref} ->
	    lager:info("zab engine start db on log dir ~p ok",[Dir]),
	    {ok, #state { leveldb = Ref }};
	{error, Reason} ->
	    lager:info("zab engine start db on log dir ~p error:",[Reason]),
	    {error, Reason}
    end.



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
handle_call({put_proposal,Zxid,Proposal,_Opts}, _From, State=#state{leveldb=Db}) ->
%    {_Epoch,TxnId}=Zxid,
    Key=zabe_util:encode_zxid(Zxid),
     case eleveldb:put(Db,list_to_binary(Key),erlang:term_to_binary(Proposal),[]) of
	ok->
	    {reply, ok, State};
	{error,Reason} ->
	    {reply, {error,Reason}, State}
    end;
handle_call({get_proposal,Zxid,_Opts}, _From, State=#state{leveldb=Db}) ->
   % {_Epoch,TxnId}=Zxid,
   % Key=integer_to_list(TxnId),
    Key=zabe_util:encode_zxid(Zxid),
    case eleveldb:get(Db,list_to_binary(Key),[]) of
	{ok,Value}->
	    {reply,{ok,erlang:binary_to_term(Value)}, State};
	not_found->
	    {reply,not_found, State};
	{error,Reason} ->
	    {reply, {error,Reason}, State}
    end;
handle_call({get_last_proposal,_Opts}, _From, State=#state{leveldb=Db}) ->
    {ok,Itr}=eleveldb:iterator(Db,[]),
    Zxid=case eleveldb:iterator_move(Itr,last) of
	     {error,invalid_iterator}->
		 not_found;
	     {error,iterator_closed}->
		 not_found;
	     {ok,Key,_Value}->
		 zabe_util:decode_zxid(binary_to_list(Key));
	     {ok,Key}->
		 zabe_tuil:decode_zxid(binary_to_list(Key))
	 end,
    {reply,{ok,Zxid},State};
handle_call({get_epoch_last_zxid,Epoch,_Opts}, _From, State=#state{leveldb=Db}) ->
    {ok,Itr}=eleveldb:iterator(Db,[]),
    Start={Epoch+1,0},
    eleveldb:iterator_move(Itr,list_to_binary(zabe_util:encode_zxid(Start))),
    Zxid=case eleveldb:iterator_move(Itr,prev) of
	     {error,invalid_iterator}->
		 not_found;
	     {error,iterator_closed}->
		 not_found;
	     {ok,Key,_Value}->
		 zabe_util:decode_zxid(binary_to_list(Key));
	     {ok,Key}->
		 zabe_tuil:decode_zxid(binary_to_list(Key))
	 end,
    {reply,{ok,Zxid},State};
handle_call({fold,Fun,Start,_Opts}, _From, State=#state{leveldb=Db}) ->
    
    L=eleveldb:fold(Db,
		       Fun,
		       [], [{first_key, list_to_binary(zabe_util:encode_zxid(Start))}]),

    {reply,{ok,L},State}.




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
handle_info(_Info, State) ->
    {noreply, State}.

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
