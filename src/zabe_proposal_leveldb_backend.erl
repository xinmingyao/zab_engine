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



-export([put_proposal/3,get_proposal/2,get_last_proposal/1,fold/4,get_epoch_last_zxid/2]).
-export([delete_proposal/2,gc/2,get_gc/1]).



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

gc(Max,Opts)->
    gen_server:cast(?SERVER,{gc,Max,Opts}).

get_gc(Opts)->
    gen_server:call(?SERVER,{get_gc,Opts}).


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



init([Dir,_Opts]) ->    
   % WorkDir="/home/erlang/tmp/proposal.bb",
   % Bucket        = proplists:get_value(bucket,Opts,""),
   % eleveldb:repair(Dir,[]),
    case eleveldb:open(Dir, [{create_if_missing, true},{max_open_files,50},
			     {comparator,zab,
			     %"/tmp/22"
			      filename:join(Dir,"gc_db")
			     }]) of
        {ok, Ref} ->
	    lager:info("zab engine start db on log dir ~p ok",[Dir]),
	    %%the bigest key in db ,other else leveldb iterator to Max Key will error
	    K1= zabe_zxid:max_key(255),
	    case eleveldb:get(Ref,K1,[]) of
	     	not_found->
	    	    eleveldb:put(Ref,K1,K1,[]);
                 {error,Reason}->
	    	    lager:info("erorr ~p",[Reason]);
	    	_->
	    	    ok
             end ,
	    {ok, #state { leveldb = Ref}};
	{error, Reason} ->
	    lager:error("zab engine start db on log dir ~p error:",[Reason]),
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
    Bucket = proplists:get_value(bucket,Opts,1),
    case eleveldb:put(Db,zabe_zxid:encode_key(Bucket,Zxid),erlang:term_to_binary(Proposal),[]) of
	ok->
	    {reply, ok, State};
	{error,Reason} ->
	    {reply, {error,Reason}, State}
    end;
handle_call({get_proposal,Zxid,Opts}, _From, State=#state{leveldb=Db}) ->
  
    Bucket = proplists:get_value(bucket,Opts,1),
    case eleveldb:get(Db,zabe_zxid:encode_key(Bucket,Zxid),[]) of
	{ok,Value}->
	    {reply,{ok,erlang:binary_to_term(Value)}, State};
	not_found->
	    {reply,not_found, State};
	{error,Reason} ->
	    {reply, {error,Reason}, State}
    end;

handle_call({delete_proposal,Zxid,Opts}, _From, State=#state{leveldb=Db}) ->
    Bucket = proplists:get_value(bucket,Opts,1),
    case eleveldb:delete(Db,zabe_zxid:encode_key(Bucket,Zxid),[]) of
	ok->
	    {reply,ok, State};
	{error,Reason} ->
	    {reply, {error,Reason}, State}
    end;

handle_call({get_last_proposal,Opts}, _From, State=#state{leveldb=Db}) ->
    Bucket = proplists:get_value(bucket,Opts,1),
    {ok,Itr}=eleveldb:iterator(Db,[]),
    K1=zabe_zxid:max_key(Bucket),   
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
			  
			  case zabe_zxid:decode_key(Key1) of
			      {ok,Bucket,Z}->
				  Z;
			      _ ->
				 not_found
			 end
		 end
	 end
    ,
    {reply,{ok,Zxid},State};
handle_call({get_epoch_last_zxid,Epoch,Opts}, _From, State=#state{leveldb=Db}) ->
    Bucket = proplists:get_value(bucket,Opts,1),
    {ok,Itr}=eleveldb:iterator(Db,[]),
    %look up epoch+1 and previous key is the last key of epoch
    Start={Epoch+1,0},
    Zxid=case eleveldb:iterator_move(Itr,zabe_zxid:encode_key(Bucket,Start)) of
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
			  case zabe_zxid:decode_key(Key) of
			      {ok,Bucket,Z}->
				  Z;
			      _ ->
				 not_found
			 end

		 end
	 end,
    {reply,{ok,Zxid},State};

handle_call({get_gc,Opts}, _From, State=#state{leveldb=Db}) ->
    Bucket = proplists:get_value(bucket,Opts,1),
    GcKey=zabe_zxid:gc_key(Bucket),
    case eleveldb:get_gc(Db,GcKey,[]) of
	{ok,Value}->
	    {reply,zabe_zxid:decode_key(Value), State};
	not_found->
	    {reply,not_found, State};
	{error,Reason} ->
	    {reply, {error,Reason}, State}
    end;

handle_call({fold,Fun,Acc,Start,Opts}, _From, State=#state{leveldb=Db}) ->
        Bucket = proplists:get_value(bucket,Opts,1),
%    L=eleveldb:fold(Db,
%		       Fun,
%		       [], [{first_key, list_to_binary(zabe_util:encode_key(zabe_util:encode_zxid(Start),Bucket))}]),
    L=eleveldb_util:zxid_fold(Db,Fun,Acc,Start,Bucket),
    {reply,{ok,L},State}.



%%--------------------------------------------------------------------GcKey=zabe_zxid:gc_key(Bucket),
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({gc,Max,Opts}, State=#state{leveldb=Db}) ->
    Bucket = proplists:get_value(bucket,Opts,1),
    GcKey=zabe_zxid:gc_key(Bucket),
    GcValue=zabe_zxid:encode_key(Bucket,Max),
    eleveldb:put_gc(Db,GcKey,GcValue,[]),	
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
