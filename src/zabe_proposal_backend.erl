-module(zabe_proposal_backend).
-include("zabe_main.hrl").

-type opts()::[
	       {prefix,V::string()}
	      ].
%%%=========================================================================
%%%  API
%%%=========================================================================
-callback start(Dir::string(),Opts::opts())->
    ok.
-callback get_last_proposal(Opts::opts())->
    {ok,Zxid::zxid()}|not_found.
-callback put_proposal(Zxid :: zxid(),Proposal::#proposal{},Opts::[]) ->
    ok.
-callback get_proposal(Zxid :: zxid(),Opts::[]) ->
    {ok, Proposal::#proposal{}} |
    {error,not_found} .
-callback   get_epoch_last_zxid(Epoch::non_neg_integer(),Opts::[])->
    {ok,Zxid::zxid()}.
-callback fold(Fun::fun(),StartZxid::zxid(),Opts::[])->
    ok.

-callback delete_proposal(Key::zxid(),Opts::[])->
    ok|{error,Reason::any()}.

-callback gc(GcMinZxid::zxid(),GcMaxZxid::zxid(),Opts::[])->
    ok.
  
