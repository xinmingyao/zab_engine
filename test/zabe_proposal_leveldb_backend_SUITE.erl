%%%-------------------------------------------------------------------
%%% @author  <yaoxinming@gmail.com>
%%% @copyright (C) 2012, 
%%% @doc
%%%
%%% @end
%%% Created : 19 Jun 2012 by  <>
%%%-------------------------------------------------------------------
-module(zabe_proposal_leveldb_backend_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include("zabe_main.hrl").


suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    Config
.

end_per_suite(_Config) ->

    ok.

init_per_group(_GroupName, Config) ->
    Config.


end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    os:cmd("rm -rf /tmp/proposal.bb"),
    timer:sleep(2), 
    {ok,Pid}=zabe_proposal_leveldb_backend:start_link("/tmp/proposal.bb",[{prefix,1}]),
%    {ok,Pid}=zabe_proposal_leveldb_backend:start_link("/home/erlang/opensource/tiger/dev/dev2/Proposal",[{prefix,1}]),
    [{pid,Pid}|Config]

    .

end_per_testcase(_TestCase, Config) ->
    Pid=?config(pid,Config),
    erlang:exit(Pid,normal),        
    ok.

groups() ->
    [].

all() -> 
    [put_get,get_last,zxid_fold,get_epoch_last_zxid].

put_get(_Config) ->
    Zxid={1,1},
    Txn=#transaction{zxid=Zxid,value=test},
    Proposal=#proposal{transaction=Txn},
    ok=zabe_proposal_leveldb_backend:put_proposal(Zxid,Proposal,[]),
    {ok,Proposal}=zabe_proposal_leveldb_backend:get_proposal(Zxid,[]),
    ok.

get_last(_Config)->
    Zxid={10,10},
    Txn=#transaction{zxid=Zxid,value=test},
    Proposal=#proposal{transaction=Txn},
    Z2={20,20},
    zabe_proposal_leveldb_backend:put_proposal(Zxid,Proposal,[]),
    zabe_proposal_leveldb_backend:put_proposal(Z2,Proposal,[]),
    {ok,Last}=zabe_proposal_leveldb_backend:get_last_proposal([]),
    Z2=Last,
    ok.
get_epoch_last_zxid(_Config)->
    Zxid={10,10},
    Txn=#transaction{zxid=Zxid,value=test},
    Proposal=#proposal{transaction=Txn},
    Z2={10,11},
    Txn2=#transaction{zxid=Z2,value=test},
    Proposal2=#proposal{transaction=Txn2},
    Z3={20,20},
    Txn3=#transaction{zxid=Z3,value=test},
    Proposal3=#proposal{transaction=Txn3},
    zabe_proposal_leveldb_backend:put_proposal(Zxid,Proposal,[]),
    zabe_proposal_leveldb_backend:put_proposal(Z2,Proposal2,[]),
    zabe_proposal_leveldb_backend:put_proposal(Z3,Proposal3,[]),
    {ok,Z2}=zabe_proposal_leveldb_backend:get_epoch_last_zxid(10,[]),
    ok.

zxid_fold(_Config)->
    Zxid={10,10},
    Txn=#transaction{zxid=Zxid,value=test},
    Proposal=#proposal{transaction=Txn},
    Z2={10,21},
    Txn2=#transaction{zxid=Z2,value=test},
    Proposal2=#proposal{transaction=Txn2},
    Z3={20,20},
    Txn3=#transaction{zxid=Z3,value=test},
    Proposal3=#proposal{transaction=Txn3},
    zabe_proposal_leveldb_backend:put_proposal(Zxid,Proposal,[]),
    zabe_proposal_leveldb_backend:put_proposal(Z2,Proposal2,[]),
    zabe_proposal_leveldb_backend:put_proposal(Z3,Proposal3,[]),
    {ok,{L2,_}}=zabe_proposal_leveldb_backend:fold(fun({Key,_Value},{Acc,Count})->
						       {[Key|Acc],Count} end,{[],infinite},{20,20},[]),
    1=length(L2),
    {ok,{L3,_}}=zabe_proposal_leveldb_backend:fold(fun({Key,_Value},{Acc,Count})->		
				   {[Key|Acc],Count-1} end,{[],3},{10,10},[]),
    3=length(L3),
    L3=[Z3,Z2,Zxid],
    ok.

put(254) ->
    ok;
put(N)->
    Z={1,N},
    Txn=#transaction{zxid=Z,value=test},
    Proposal=#proposal{transaction=Txn},
    zabe_proposal_leveldb_backend:put_proposal(Z,Proposal,[]),
    put(N-1).
%% test leveldb zxid encode decode relate for byte endian
zxid_f2(_Config)->   
    put(257),
    {ok,_}=zabe_proposal_leveldb_backend:get_proposal({1,256},[]),
    {ok,{L3,_}}=zabe_proposal_leveldb_backend:fold(fun({Key,_Value},{Acc,Count})->{[Key|Acc],Count-1} end,{[],3},{1,1},[]),
    [{1,257},{1,256},{1,255}]=L3,
    ok.

gc(_Conf)->
    Gc={10,20},
    timer:sleep(5),
    zabe_proposal_leveldb_backend:gc(Gc,[]),
    {ok,_,Gc}=zabe_proposal_leveldb_backend:get_gc([]),
    ok
	.
zxid_fold_count(_Config)->
    Zxid={10,10},
    Txn=#transaction{zxid=Zxid,value=test},
    Proposal=#proposal{transaction=Txn},
    Z2={10,11},
    Txn2=#transaction{zxid=Z2,value=test},
    Proposal2=#proposal{transaction=Txn2},
    Z3={20,20},
    Txn3=#transaction{zxid=Z3,value=test},
    Proposal3=#proposal{transaction=Txn3},

    Z4={20,21},
    Txn4=#transaction{zxid=Z4,value=test},
    Proposal4=#proposal{transaction=Txn4},

    zabe_proposal_leveldb_backend:put_proposal(Zxid,Proposal,[]),
    zabe_proposal_leveldb_backend:put_proposal(Z2,Proposal2,[]),
    zabe_proposal_leveldb_backend:put_proposal(Z3,Proposal3,[]),
    zabe_proposal_leveldb_backend:put_proposal(Z4,Proposal4,[]),
%    {ok,L1}=zabe_proposal_leveldb_backend:iterate_zxid_count(fun({_K,V})->
%									V end,Zxid,2),

     {ok,{L3,_}}=zabe_proposal_leveldb_backend:fold(fun({Key,_Value},{Acc,Count})->		
				   {[Key|Acc],Count-1} end,{[],2},Zxid,[]),
    
    2=length(L3),

 %   [Proposal,Proposal2]=L1,
    {ok,{[Proposal4,Proposal3],_}}=
	zabe_proposal_leveldb_backend:fold(fun({_Key,Value},{Acc,Count})->		
				   {[Value|Acc],Count-1} end,{[],2},zabe_util:zxid_plus(Z2),[]),
    

    ok.
