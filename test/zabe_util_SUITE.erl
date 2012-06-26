-module(zabe_util_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-compile([{parse_transform, lager_transform}]).
-include("zabe_main.hrl").

init_per_suite(Config)->


    Config    
.



end_per_suite(Config)->

    Config.

init_per_testcase(_,Config)->

    Config
    .

end_per_testcase(Config)->

    ok.




all()->
    [e_2_s,encode,decode].
encode(_)->
    Zxid=zabe_util:encode_zxid({1,2}),
    "000000000100000000000000000002"=Zxid,
    {1,2}=zabe_util:decode_zxid(Zxid).
e_2_s(_C)->
    I="1",
    "01"=zabe_util:e_2_s(I,2),
    try zabe_util:e_2_s(1,2) of
	_->ct:fail("should error")
    catch
	_:_->ok
    end,
    "0000000001"=zabe_util:epoch_to_string(1),
    "00000000000000000001"=zabe_util:txn_to_string(1).

zxid_compare(Config)->
    equal=zabe_util:zxid_compare({0,0},{0,0}),
    big=zabe_util:zxid_compare({1,2},{1,1}),
    epoch_small=zabe_util:zxid_compare({1,2},{2,1}),
    ok.


    

    


