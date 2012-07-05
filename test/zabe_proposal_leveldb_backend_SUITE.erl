%%%-------------------------------------------------------------------
%%% @author  <>
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

end_per_suite(Config) ->

    ok.

init_per_group(_GroupName, Config) ->
    Config.


end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    os:cmd("rm -rf /tmp/proposal.bb"),
    timer:sleep(2),
    {ok,Pid}=zabe_proposal_leveldb_backend:start_link("/tmp/proposal.bb",[{prefix,"100"}]),
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

put_get(Config) ->
    Zxid={1,1},
    Txn=#transaction{zxid=Zxid,value=test},
    Proposal=#proposal{transaction=Txn},
    ok=zabe_proposal_leveldb_backend:put_proposal(Zxid,Proposal,[]),
    {ok,Proposal}=zabe_proposal_leveldb_backend:get_proposal(Zxid,[]),
    ok.

get_last(Config)->
    Zxid={10,10},
    Txn=#transaction{zxid=Zxid,value=test},
    Proposal=#proposal{transaction=Txn},
    Z2={20,20},
    zabe_proposal_leveldb_backend:put_proposal(Zxid,Proposal,[]),
    zabe_proposal_leveldb_backend:put_proposal(Z2,Proposal,[]),
    {ok,Last}=zabe_proposal_leveldb_backend:get_last_proposal([]),
    Z2=Last,
    ok.
get_epoch_last_zxid(Config)->
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

zxid_fold(Config)->
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
    {ok,{L2,_}}=zabe_proposal_leveldb_backend:fold(fun({Key,_Value},{Acc,Count})->
						       {[Key|Acc],Count} end,{[],infinite},{20,20},[]),
    1=length(L2),
    {ok,{L3,_}}=zabe_proposal_leveldb_backend:fold(fun({Key,_Value},{Acc,Count})->		
				   {[Key|Acc],Count} end,{[],infinite},{10,11},[]),
    2=length(L3),
    ok.

zxid_fold_count(Config)->
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
