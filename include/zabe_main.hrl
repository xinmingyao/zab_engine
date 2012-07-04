-type zxid()::{Epoch::non_neg_integer(),TxnId::non_neg_integer()}.
-type peer()::{Name::atom(),Node::node()}.
-type client()::{Name::atom(),Node::node()}|{Pid::pid(),Node::node()}.


-define(ELECT_CMD,1).
-define(VOTE_CMD,2).
-define(PROPOSE_CMD,3).
-define(TICK_CMD,4).
-define(LEARN_CMD,5).
-define(ZAB_CMD,6).

-define(LOOKING,1).
-define(LEADING,2).
-define(FOLLOWING,3).
-define(LEADER_RECOVER,4).
-define(FOLLOWER_RECOVER,5).
-define(OBSERVING,6).




-type cmd()::?ELECT_CMD|?VOTE_CMD.


-record(vote,{from::node()
	      ,leader::node()
	      ,zxid::zxid()
	      ,epoch
	      ,state::?LOOKING|?LEADING|?OBSERVING
	      ,last_commit_zxid::zxid()	      
	     }).


-record(election, {
	  parent ::atom(),
          ensemble = []      :: [node()],
	  last_zxid ::zxid(),
	  quorum::non_neg_integer(),
	  last_commit_zxid::zxid()
         }).

-record(msg,{cmd::cmd(),
	     value::any()
}).

-record(transaction,{value::any(),zxid::zxid()}).


-record(proposal,{sender::peer(),client::client(),transaction::#transaction{}}).

-record(zab_req,{msg}).
-record(zab_ack,{msg}).
-record(zab_commit,{msg}).

-record(zab_server_info,{leader,zab_state}).

-record(proposal_rec,{zxid::zxid(),proposal::#proposal{},acks::list(),commit::boolean()}).



