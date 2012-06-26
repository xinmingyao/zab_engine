-module(zabe_util).
-export([e_2_s/2,decode_zxid/1,encode_zxid/1]).
-export([zxid_eq/2,zxid_big/2,zxid_plus/1,zxid_compare/2]).
-export([epoch_to_string/1,txn_to_string/1]).

-include("zabe_main.hrl").

-spec zxid_compare(Z1::zxid(),Z2::zxid())->
			  equal|big|epoch_smsall.
zxid_compare(Z1={E1,_T1},Z2={E2,_T2})->
    case zxid_eq(Z1,Z2) of
	true->
	    equal;
	false ->
	    if
		E1<E2-> epoch_small ;
		true -> big
	    end
    end.

		    
zxid_plus({E,T})->
    {E,T+1}.

zxid_eq({E1,T1},{E2,T2})->
    E1==E2 andalso T1==T2.

zxid_big({E1,T1},{E2,T2})->
    E1 >E2 or ((E1==E2) andalso T1>T2).
encode_zxid({Epoch,Txn})->
    epoch_to_string(Epoch)++txn_to_string(Txn).
decode_zxid(Zxid)->
    Epoch=string:substr(Zxid,1,10),
    Txn=string:substr(Zxid,11,20),
    {erlang:list_to_integer(Epoch),list_to_integer(Txn)}.
	 

epoch_to_string(Epoch) when is_integer(Epoch)->
    e_2_s(erlang:integer_to_list(Epoch),10)
    .


txn_to_string(Txn) when is_integer(Txn)->
    e_2_s(erlang:integer_to_list(Txn),20).

e_2_s(S,L) when length(S)=:=L->
    S;
e_2_s(S,L) when length(S) < L ->
    e_2_s([$0|S],L);
e_2_s(_,_) ->
    exit(epoch_errror).


