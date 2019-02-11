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
-module(zabe_log_gc).

-behaviour(gen_server).

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {nodes::[node()],
		master::atom(),
		prefix::string(),
		logic_time::integer(),
		gc_replys::dict:new()}).

-export([get_last_gc_zxid/1,gc/1]).

-include("zabe_main.hrl").


%%%===================================================================
%%% API
%%%===================================================================
-spec get_last_gc_zxid(P::pid())->
	{ok,Zxid::string()}|{error,Reason::any()}.
get_last_gc_zxid(P)->
    gen_server:call(P,{get_last_gc_zxid}).

gc(P)->
    gen_server:cast(P,{gc}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Nodes,Pid,Prefix,LogicTime) ->
    gen_server:start_link(?MODULE, [Nodes,Pid,Prefix,LogicTime], []).

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
init([Nodes,Pid,Prefix,LogicTime]) ->
    case mnesia:create_schema([node()]) of
	ok->
	    mnesia:start(),
	    {atomic,ok}=mnesia:create_table(log_gc,[{type,ordered_set},{attributes,record_info(fields,log_gc)}]); 
	_->
	    mnesia:start()
    end,
    {ok, #state{logic_time=LogicTime,nodes=Nodes,master=Pid,prefix=Prefix,gc_replys=dict:new()}}.

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
handle_call({get_last_gc_zxid}, _From, State) ->
    Reply = ok,
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
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
handle_cast({gc}, State=#state{nodes=Nodes,master=Mod,logic_time=LogicTime}) ->
    Msg=#gc_req{from=self()},
    [send_zab_msg({Mod,Node},Msg,LogicTime)||Node<-Nodes],
    {noreply,State};
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
handle_info(M=#gc_reply{from=From},State=#state{prefix=Prefix,nodes=Nodes,gc_replys=GcReplys}) ->
    G2=dict:store(From,M,GcReplys),
    T1=dict:size(G2),
    if T1=:=length(Nodes) ->
	   
	    Min=zabe_util:min(Prefix),
	    LastCommit=dict:fold(fun({_K,#gc_reply{last_commit_log_zxid=Zxid}},Acc)->
					 T=zabe_util:zxid_big(Zxid,Acc),
					 if 
					     T->
						 Zxid;
					     true->Acc
					 end end
					 ,{0,0},G2),
	    Last=zabe_util:encode_key(zabe_util:encode_zxid(LastCommit),Prefix),
	    Rec=#log_gc{prefix=Prefix,min=Min,max=Last},
	    % notify  proposal backend gc
	    %
	    %
	    % ProposalBackEnd:gc(Prefix,Min,Last),

	    %log local
	    mnesia:transaction(fun()->mnesia:write(Rec) end),

	    {noreply, State#state{gc_replys=dict:new()}}
	    ;
       true->
	    {noreply, State#state{gc_replys=G2}}
       end;
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

send_zab_msg(To,Msg,LC)->
    catch erlang:send(To,#msg{cmd=?ZAB_CMD,value=Msg,epoch=LC}).
