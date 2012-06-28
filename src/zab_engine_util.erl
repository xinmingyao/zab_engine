-module(zab_engine_util).
-export([get_env/2]).

-spec get_env(atom(), term()) -> term().
get_env(Key, Default) ->
    case application:get_env(zab_engine, Key) of
	{ok, Value} -> {ok, Value};
	undefined   -> {ok, Default}
    end.

