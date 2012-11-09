-module(zabe_zxid).
-compile([export_all]).


-ifdef(BUCKET64).
-define(BUCKET_LENGTH,64).
-else.
-define(BUCKET_LENGTH,8).
-endif.

-define(EPOCH,24).
-define(TXN_ID,40).
-include("zabe_main.hrl").

-type zab_key()::'binary list as below'.
%<<bucket:?BUCKET_LENGTH,Epoch::integer:EPOCH,TXN_ID::integer():?TXN_ID>>
-export([encode_key/2,decode_key/1,gc_key/1]).

-spec encode_key(Bucket::integer(),Zxid::zxid())->
			ZabKey::zab_key().
encode_key(Bucket,Zxid={Epoch,TxnId})->
%    error_logger:info_msg("~p ~p 111111111111~n",[Bucket,Zxid]),
    <<Bucket:?BUCKET_LENGTH/little,
      Epoch:?EPOCH/little,
      TxnId:?TXN_ID/little>>
          .
-spec decode_key(Bin::binary())->
			{ok,Bucket::integer(),Zxid::zxid()}.
decode_key(Bin)->    
    <<Bucket:?BUCKET_LENGTH/little,
      Epoch:?EPOCH/little,
      TxnId:?TXN_ID/little>> =Bin,
    {ok,Bucket,{Epoch,TxnId}}.

max_key(Bucket)->
    
    encode_key(Bucket,max_zxid()).
max_zxid()->
    %2^24
    Epoch=16777216-1,
    %2^40
    TxnId=1099511627776-1,
    {Epoch,TxnId}.
    
gc_key(Bucket)->
    GcBucket=0,
    GcEpoch=0,
    GcTxnId=Bucket,
    encode_key(GcBucket,{GcEpoch,GcTxnId}).
    
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
codec_test()->
    Bucket=1,
    Epoch=2,
    TxnId=3,
    Bin=encode_key(Bucket,{Epoch,TxnId}),
    ?assertEqual({ok,Bucket,{Epoch,TxnId}},decode_key(Bin)).
max_key_test()->
   Bucket=1,
   ?assertEqual({ok,Bucket,max_zxid()},decode_key(max_key(Bucket))).

-endif.



