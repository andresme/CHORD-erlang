%%%-------------------------------------------------------------------
%%% File    : chord.erl
%%% Author  : JCastro <jose.r.castro@gmail.com>
%%% Description : 
%%%
%%% Created : 28 Aug 2014 by JCastro <>
%%%-------------------------------------------------------------------
-module(chord).

-behaviour(gen_server).

%% API
-export([start/4,join_ring/4, get_state/1]).

-export([find_successor/2,get_fingerTable/1,stabilize/1,fix_fingers/1,check_pred/1,notify/2,add_key/3,
	 get_value/2,del_key/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(id,
	{
	  name=none,
	  hash=0
	}).

-record(state, {id, m, n, succ, pred, timer, next}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start(Name, M, TicMin, TicMax) ->
    Timer = spawn(timer,clock,[Name, TicMin, TicMax]),
    gen_server:start_link({local, Name}, ?MODULE, {new_ring, Name, Timer, M}, []).
    
join_ring(Name, Other, TicMin, TicMax) ->
    Timer = spawn(timer,clock,[Name, TicMin, TicMax]),
    gen_server:start_link({local, Name}, ?MODULE, {join_ring, Name, Timer, Other}, []).

find_successor(Name, Id) ->
    gen_server:cast(Name, {find_successor, self(), Id}),
    receive
	{find_successor, Succ} -> Succ
    end.

get_server(Name) ->
	gen_server:cast(Name, {get_server, self()}),
	receive
		{get_server, State} -> 
			State
	end.
	
is_pred_alive(Name) ->
	gen_server:cast(Name, {check_alive, self()}),
	receive
		{alive, _} -> 
			alive
		after 1000 -> death
	end.
	
get_state(Name) ->
	gen_server:cast(Name, {get_server, self()}),
	receive
		{get_server, State} -> 
			io:format("==========STATE==========~n"),
			io:format("Name: ~w~n", [State#state.id#id.name]),
			io:format("Hash: ~w~n", [State#state.id#id.hash]),
			io:format("M: ~w~n", [State#state.m]),
			io:format("N: ~w~n", [State#state.n]),
			io:format("Succ: ~w~n", [State#state.succ]),
			io:format("Pred: ~w~n", [State#state.pred]),
			io:format("Next: ~w~n", [State#state.next]),
			io:format("==========STATE==========~n")
	end.

add_key(_Name, _Key, _Value) ->
    io:format("add_key not implemented yet~n"),
    not_implemented.

get_value(_Name, _Key) ->
    io:format("get_value not implemented yet~n"),
    not_implemented.

del_key(_Name, _Key) ->
    io:format("del_key not implemented yet~n"),
    not_implemented.


stabilize  (Name) -> gen_server:cast(Name, stabilize).
fix_fingers(Name) -> gen_server:cast(Name, fix_fingers).
check_pred (Name) -> gen_server:cast(Name, check_pred).

notify(Name, Pred) -> gen_server:cast(Name, {notify, Pred}).

get_fingerTable(Name) ->
    gen_server:call(Name, get_fingerTable).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------

init({Type, Name, Timer, M_Other}) ->
	case Type of 
	new_ring ->
		N = pot2(M_Other),
		Me = #id{name=Name, hash=erlang:phash2(Name) rem N},
		put({finger,1}, Me),
		createFingerTable(2, M_Other),
		{ok, 
			#state{
				id    = Me,
				m     = M_Other,
				n     = N,
				succ  = Me,
				pred  = nil,
				timer = Timer,
				next  = 1
			}
		};
	join_ring ->
		Other = get_server(M_Other),
		M = Other#state.m,
		N = pot2(M),
		Me = #id{name=Name, hash=erlang:phash2(Name) rem N},
		put({finger, 1}, Me),
		createFingerTable(2, M),
		{ok, 
			#state{
				id    = Me,
				m     = M,
				n     = N,
				succ  = find_successor(M_Other, Other#state.id),
				pred  = nil,
				timer = Timer,
				next  = 1
			}
		}
	end.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(get_fingerTable, _From, State) ->
    Reply = get_fingerTable(1,State#state.m),
    {reply, Reply, State};

handle_call(Request, _From, State) ->
    io:format("request ~w NOT implemented by ~w~n",[Request,State#state.id#id.name]),
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({find_successor, Proc, Id}, State) ->
    find_succ(Proc, Id, State),
    {noreply, State};
    
handle_cast({get_server, Proc}, State) ->
    get_serv(Proc, State),
    {noreply, State};
    
handle_cast({check_alive, Proc}, State) -> 
	check_alive_status(Proc, State),
	{noreply, State};

handle_cast(stabilize,   State) -> 
	io:format("~w stabilize~n",[State#state.id]),
	TempSucc = State#state.succ,
	if TempSucc == State#state.id ->
			X = State;
		true ->
			X = get_server(TempSucc#id.name)
	end,
	if X#state.pred /= nil ->
		if X#state.pred == State#state.id ->
			X2 = State;
		true ->
			X2 = get_server(X#state.pred#id.name)
		end,
		XHash = X2#state.id#id.hash,
		if XHash < State#state.succ#id.hash, XHash > State#state.id#id.hash ->
			State2 = State#state{succ = X2#state.id};
		State#state.id#id.hash == State#state.succ#id.hash ->
			State2 = State#state{succ = X2#state.id};
		State#state.id#id.hash > State#state.succ#id.hash, State#state.succ#id.hash > XHash ->
			State2 = State#state{succ = X2#state.id};
		true ->
			State2 = State
		end;
	true -> 
		State2 = State,
		io:format("succ.pred = nil~n")
	end,
	notify(State2#state.succ#id.name, State2#state.id),
	{noreply, State2};

handle_cast(fix_fingers, State) -> 
	io:format("~w fix_fingers~n",[State#state.id]), 
	{noreply, State};
	



handle_cast(check_pred,  State) -> 
	io:format("~w check_pred~n",[State#state.id]),
	if State#state.pred == nil; State#state.id == State#state.pred ->
		State2 = State;
		true ->
			Status = is_pred_alive(State#state.pred#id.name),
			case Status of
			alive -> 
				State2 = State;
			death ->
				State2 = State#state{pred = nil}
			end
	end,
	{noreply, State2};

handle_cast({notify,Pred}, State) -> 
	if Pred /= nil, State#state.pred == nil ->
		io:format("Pred is nil~n"),
		Pred_id = #id{name=Pred#id.name, hash=Pred#id.hash},
		StateNew = State#state{pred = Pred_id};
	Pred /= nil, Pred#id.hash > State#state.pred#id.hash, Pred#id.hash < State#state.id#id.hash ->
		io:format("New Pred found~n"),
		Pred_id = #id{name=Pred#id.name, hash=Pred#id.hash},
		StateNew = State#state{pred = Pred_id};
	Pred /= nil, Pred#id.hash < State#state.pred#id.hash, State#state.pred#id.hash == State#state.id#id.hash ->
		io:format("New Pred found~n"),
		Pred_id = #id{name=Pred#id.name, hash=Pred#id.hash},
		StateNew = State#state{pred = Pred_id};
	Pred /= nil, Pred#id.hash < State#state.pred#id.hash, State#state.pred#id.hash > State#state.id#id.hash ->
		io:format("New Pred found~n"),
		Pred_id = #id{name=Pred#id.name, hash=Pred#id.hash},
		StateNew = State#state{pred = Pred_id};
	true -> 
		io:format("No new pred found~n"),
		StateNew = nil
	end,
	if StateNew /= nil ->
		{noreply, StateNew};
	true ->
		{noreply, State}
	end.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
    State#state.timer ! stop,
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
find_succ(Proc, _Id,#state{id=#id{name=Me,hash=Hash}}) ->
    Proc ! {find_successor, #id{name=Me, hash=Hash}}.

get_serv(Proc, State=#state{id=#id{name=_,hash=_}}) ->
	Proc ! {get_server, State}.
	
check_alive_status(Proc, State) ->
	Proc ! {alive, State}.

pot2(0) -> 1;
pot2(N) -> 2*pot2(N-1).

createFingerTable(K,M) when K > M -> ok;
createFingerTable(K,M) ->
    put({finger,K}, #id{}),
    createFingerTable(K+1,M).

get_fingerTable(I,M) when I > M -> [];
get_fingerTable(I,M) -> [{I,get({finger,I})}|get_fingerTable(I+1,M)].
