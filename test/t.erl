-module(t).
-export([start/0]).

start()->
    
    Dir="/tmp/p3.db",
    {ok,Db}=eleveldb:open(Dir, [{create_if_missing, true},{max_open_files,50}]),
    {ok,It}=eleveldb:iterator(Db,[]),
    error_logger:info_msg("dddd~p",[eleveldb:iterator_move(It,first)]).
s2()->
        Dir="/tmp/p1.db",
    zabe_proposal_leveldb_backend:start_link(Dir,[]),
    zabe_proposal_leveldb_backend:get_last_proposal([]).
s3()->
    Dir="/tmp/p3.db",
    zabe_proposal_leveldb_backend:start_link(Dir,[]),
    zabe_proposal_leveldb_backend:iterate_zxid_count(fun({_K,V})->
				       V end,{1,0},100).
