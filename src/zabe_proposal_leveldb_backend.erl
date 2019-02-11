%%%-------------------------------------------------------------------
%%% @author  <yaoxinming@gmail.com>
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
-export([start_link/2,start/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 
-include("zabe_main.hrl").
-compile([{parse_transform, lager_transform}]).

-record(state, {leveldb}).
-define(MAX_PROPOSAL,"999999999999999999999999999999").


-export([put_proposal/3,get_proposal/2,get_last_proposal/1,fold/4,get_epoch_last_zxid/2]).
-export([delete_proposal/2,gc/3]).



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
delete_proposal(Key,Opts)->
    gen_server:call(?SERVER,{delete_proposal,Key,Opts})   .

fold(Fun,Acc,Start,Opts)->
    gen_server:call(?SERVER,{fold,Fun,Acc,Start,Opts},infinity).
get_epoch_last_zxid(Epoch,Opts)->
    gen_server:call(?SERVER,{get_epoch_last_zxid,Epoch,Opts}).

gc(Min,Max,Opts)->
    gen_server:cast(?SERVER,{gc,Min,Max,Opts}).



%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start(Dir)->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Dir], []).
start_link(Dir,Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Dir,Opts], []).

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
-define(INIT_MAX,<<"zzzzzzzzzzzzzzzzzzz">>).


init([Dir,_Opts]) ->    
   % WorkDir="/home/erlang/tmp/proposal.bb",
   % Prefix        = proplists:get_value(prefix,Opts,""),
   % eleveldb:repair(Dir,[]),
    case eleveldb:open(Dir, [{create_if_missing, true},{max_open_files,50}]) of
        {ok, Ref} ->
	    lager:info("zab engine start db on log dir ~p ok",[Dir]),
	    
	    case eleveldb:get(Ref,?INIT_MAX,[]) of
		not_found->
		    eleveldb:put(Ref,?INIT_MAX,?INIT_MAX,[]);
                {error,Reason}->
		    lager:info("error ~p",[Reason]);
		_->
		    ok
            end ,
	    {ok, #state { leveldb = Ref}};
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
handle_call({put_proposal,Zxid,Proposal,Opts}, _From, State=#state{leveldb=Db}) ->
%    {_Epoch,TxnId}=Zxid,
    Prefix = proplists:get_value(prefix,Opts,""),
    Key=zabe_util:encode_key(zabe_util:encode_zxid(Zxid),Prefix),
     case eleveldb:put(Db,list_to_binary(Key),erlang:term_to_binary(Proposal),[]) of
	ok->
	    {reply, ok, State};
	{error,Reason} ->
	    {reply, {error,Reason}, State}
    end;
handle_call({get_proposal,Zxid,Opts}, _From, State=#state{leveldb=Db}) ->
   % {_Epoch,TxnId}=Zxid,
   % Key=integer_to_list(TxnId),
    Prefix = proplists:get_value(prefix,Opts,""),
    Key=zabe_util:encode_key(zabe_util:encode_zxid(Zxid),Prefix),
    case eleveldb:get(Db,list_to_binary(Key),[]) of
	{ok,Value}->
	    {reply,{ok,erlang:binary_to_term(Value)}, State};
	not_found->
	    {reply,not_found, State};
	{error,Reason} ->
	    {reply, {error,Reason}, State}
    end;

handle_call({delete_proposal,Zxid,Opts}, _From, State=#state{leveldb=Db}) ->
   % {_Epoch,TxnId}=Zxid,
   % Key=integer_to_list(TxnId),
    Prefix = proplists:get_value(prefix,Opts,""),
    Key=zabe_util:encode_key(zabe_util:encode_zxid(Zxid),Prefix),
    case eleveldb:delete(Db,list_to_binary(Key),[]) of
	ok->
	    {reply,ok, State};
	{error,Reason} ->
	    {reply, {error,Reason}, State}
    end;

handle_call({get_last_proposal,Opts}, _From, State=#state{leveldb=Db}) ->
    Prefix = proplists:get_value(prefix,Opts,""),
    {ok,Itr}=eleveldb:iterator(Db,[]),
%    Max= zabe_util:encode_key(?MAX_PROPOSAL,Prefix),
    Max=zabe_util:encode_zxid({999999999,1}),
    K1=list_to_binary(zabe_util:encode_key(Max,Prefix)),
   
    Zxid= case eleveldb:iterator_move(Itr,K1) of
	      {error,invalid_iterator}->
		  not_found;
	      {error,iterator_closed}->
		  not_found;
	      {ok,_Key,_}->
		  case eleveldb:iterator_move(Itr,prev) of
		     {error,invalid_iterator}->
			 not_found;
		     {error,iterator_closed}->
			 not_found;
		     {ok,Key1,_}->
			 case zabe_util:prefix_match(Key1,Prefix) of
			     true->
				 zabe_util:decode_zxid(zabe_util:decode_key(binary_to_list(Key1),Prefix));
			     false ->
				 not_found
			 end
		 end
	 end
	    
    ,

    {reply,{ok,Zxid},State};
handle_call({get_epoch_last_zxid,Epoch,Opts}, _From, State=#state{leveldb=Db}) ->
    Prefix = proplists:get_value(prefix,Opts,""),
    {ok,Itr}=eleveldb:iterator(Db,[]),
    Start=zabe_util:encode_zxid({Epoch+1,0}),
    Zxid=case eleveldb:iterator_move(Itr,list_to_binary(zabe_util:encode_key(Start,Prefix))) of
	     {error,invalid_iterator}->
		 not_found;
	     {error,iterator_closed}->
		 not_found;
	     {ok,_,_}->
		 case eleveldb:iterator_move(Itr,prev) of
		     {error,invalid_iterator}->
			 not_found;
		     {error,iterator_closed}->
			 not_found;
		     {ok,Key,_}->
			 case zabe_util:prefix_match(Key,Prefix) of
			     true->
				 zabe_util:decode_zxid(zabe_util:decode_key(binary_to_list(Key),Prefix));
			     false ->
				 not_found
			 end
		 end
	 end,
    {reply,{ok,Zxid},State};

handle_call({fold,Fun,Acc,Start,Opts}, _From, State=#state{leveldb=Db}) ->
        Prefix = proplists:get_value(prefix,Opts,""),
%    L=eleveldb:fold(Db,
%		       Fun,
%		       [], [{first_key, list_to_binary(zabe_util:encode_key(zabe_util:encode_zxid(Start),Prefix))}]),
    L=eleveldb_util:zxid_fold(Db,Fun,Acc,Start,Prefix),
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
handle_cast({gc,Min,Max,Opts}, State=#state{leveldb=Db}) ->
    Prefix = proplists:get_value(prefix,Opts,""),
    zabe_log_gc_db:gc(Prefix,Min,Max),
    eleveldb:gc(Db,Prefix,
		zabe_util:encode_key(zabe_util:encode_zxid(Min),Prefix),
		zabe_util:encode_key(zabe_util:encode_zxid(Max),Prefix)),
    {noreply, State};
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
terminate(_Reason, #state{leveldb=_Db}) ->
   % eleveldb:destroy(Db,[]),
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
