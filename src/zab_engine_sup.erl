
-module(zab_engine_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================


init([]) ->
    
    {ok,ProposalDir}=zab_engine_util:get_env(proposal_log_dir,"/tmp/proposal"),

    ProposalBackEnd = {proposal_backend,
		    {zabe_proposal_leveldb_backend, start_link,
		     [ProposalDir,[]]},
		       permanent, 5000, worker, [proposal_back_end]},
    GcDb = {log_gc_db,
		    {zabe_log_gc_db, start_link,
		     []},
		       permanent, 5000, worker, [gc_db]},

    
    % Build the process list...
    Processes = lists:flatten([
			       ProposalBackEnd,GcDb
			       
    ]),    
    % Run the processes...
    {ok, {{one_for_one, 10, 10}, Processes}}.


