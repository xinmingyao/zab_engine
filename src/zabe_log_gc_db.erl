%%%-------------------------------------------------------------------
%%% @author  <yaoxinming@gmail.com>
%%% @copyright (C) 2012, 
%%% @doc
%%%
%%% @end
%%% Created :  1 Aug 2012 by  <>
%%%-------------------------------------------------------------------
%% zab log proposal must gc,use eleveldb:gc to notice leveldb delete no use key
%% when compact
%% all node agree and gc the min zxid in nodes
-module(zabe_log_gc_db).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {nodes::[node()],
		master::atom(),
		prefix::string(),
		logic_time::integer(),
		gc_replys::dict:new()}).

-export([get_last_gc_zxid/1,gc/3]).

-include("zabe_main.hrl").
-include_lib("stdlib/include/qlc.hrl").



%%%===================================================================
%%% API
%%%===================================================================
-spec get_last_gc_zxid(string())->
	{ok,Zxid::string()}|{error,Reason::any()}.
get_last_gc_zxid(Prefix)->
    gen_server:call(?SERVER,{get_last_gc_zxid,Prefix}).

gc(Prefix,MinZxid,MinLastZxid)->
    gen_server:cast(?SERVER,{gc,Prefix,MinZxid,MinLastZxid}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local,?SERVER},?MODULE, [], []).

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
init([]) ->
    case mnesia:create_schema([node()]) of
	ok->
	    mnesia:start(),
	    {atomic,ok}=mnesia:create_table(log_gc,[{type,ordered_set},{attributes,record_info(fields,log_gc)}]); 
	_->
	    mnesia:start()
    end,
    {ok, #state{}}.

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
handle_call({get_last_gc_zxid,Prefix}, _From, State) ->
    F=fun()->
	      Q=qlc:q([R||R<-mnesia:table(log_gc),R#log_gc.prefix=:=Prefix]),
	      qlc:e(Q) end,
    
    Reply = case mnesia:transaction(F) of
		{atomic,V}->{ok,V};
		A->{error,A}
	    end,
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.


handle_cast({gc,Prefix,Min,MinCommitZxid}, State) ->
   
    Last=zabe_util:encode_key(zabe_util:encode_zxid(MinCommitZxid),Prefix),
    Rec=#log_gc{prefix=Prefix,min=zabe_util:encode_key(zabe_util:encode_zxid(Min),Prefix),max=Last},
    mnesia:transaction(fun()->mnesia:write(Rec) end),
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


