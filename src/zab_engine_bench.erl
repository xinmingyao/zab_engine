%%%-------------------------------------------------------------------
%%% @author  <>
%%% @copyright (C) 2012, 
%%% @doc
%%%
%%% @end
%%% Created : 19 Jun 2012 by  <>
%% this module is for gen_zab_engine bench test
%%%-------------------------------------------------------------------
-module(zab_engine_bench).
-behaviour(gen_zab_server). 
%% API
-export([start_link/2]).
-compile([{parse_transform, lager_transform}]).
%% gen_server callbacks
-export([init/1, handle_call/4, handle_cast/3, handle_info/3,
	 terminate/3, code_change/4]).

-define(SERVER, ?MODULE). 
-include("zabe_main.hrl").
-record(state, {leveldb}).
-export([put/2,get/1,start_bench/3]).
-export([handle_commit/4]).

%%%===================================================================
%%% API
%%%===================================================================

put(Key,Value)->
    gen_zab_server:proposal_call(?SERVER,{put,Key,Value})
    .
get(Key)->
    gen_zab_server:call(?SERVER,{get,Key}).

start_bench(Nodes,Bucket,Count)->
    lists:map(fun(Node)->
		      rpc:cast(Node,?MODULE,start_link,[Nodes,[{bucket,Bucket}]])
	      end,Nodes),
    ok,
    timer:sleep(3000),
    [H|_]=Nodes,
     erlang:statistics(wall_clock),
    loop_put(Count,H),
    {_,T2}=erlang:statistics(wall_clock),
    io:format("gen_zab_engine  bench:~p,time:~p ms ~n",[Count,T2]),
    ok.
loop_put(0,_)->
    ok;
loop_put(Count,Node) ->
    C1=Count-1,
    ok=rpc:call(Node,zab_engine_bench,put,[Count,Count]),
    loop_put(C1,Node).


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Nodes,Opts) ->
    gen_zab_server:start_link(?SERVER,Nodes,Opts, ?MODULE, [], []).

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
    {ok,#state{},{0,0}}
	.


handle_commit({put,_Key,_Value},_Zxid, State,_ZabServerInfo) ->
    {ok,ok,State}
.
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

handle_call({get,_Key}, _From, State,_) ->
	    {reply,ok, State};
handle_call(_, _From, State,_) ->
    {reply,ok,State}.

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
handle_cast(_Msg, State,_) ->
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
handle_info(_Info, State,_ZabServerInfo) ->
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
terminate(_Reason, _State,_ZabServerInfo) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra,_ZabServerInfo) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
