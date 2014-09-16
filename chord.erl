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
-export([start/4,join_ring/5]).

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
    gen_server:start_link({local, Name}, ?MODULE, {new_ring, Name, Timer, M, nil}, []).
    
join_ring(Name, M, Other, TicMin, TicMax) ->
    Timer = spawn(timer,clock,[Name, TicMin, TicMax]),
    gen_server:start_link({local, Name}, ?MODULE, {join_ring, Name, Timer, Other, M}, []).

find_successor(Name, Id) ->
	io:format("find_successor~n"),
    gen_server:cast(Name, {find_successor, self(), Id}),
    receive
		{find_successor, Succ} -> Succ
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

init({Ring, Name, Timer, M, M2}) ->
	case Ring of
	new_ring ->
		N = pot2(M),
		Me = #id{name=Name, hash=erlang:phash2(Name) rem N},
		put({finger,1}, Me),
		createFingerTable(2,M),
		{ok, 
		 #state{
		   id    = Me,
		   m     = M,
		   n     = N,
		   succ  = Me,
		   pred  = nil,
		   timer = Timer,
		   next  = 1
		  }
		};
    join_ring ->
		N = pot2(M2),
		Me = #id{name=Name, hash=erlang:phash2(Name) rem N},
		N1 = #id{name=M, hash=erlang:phash2(M) rem N},
		{ok,
		 #state{
		   id    = Me,
		   m     = M2,
		   n     = N,
		   succ  = find_successor(M, N1),
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
	io:format("handle_cast~n"),
    find_succ(Proc, Id, State),
    {noreply, State};
handle_cast(stabilize,   State)    -> io:format("~w stabilize~n",[State#state.id]),   {noreply, State};
handle_cast(fix_fingers, State)    -> io:format("~w fix_fingers~n",[State#state.id]), {noreply, State};
handle_cast(check_pred,  State)    -> io:format("~w check_pred~n",[State#state.id]),  {noreply, State};
handle_cast({notify,_Pred}, State) -> {noreply, State}.

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
	io:format("find_succ~n"),
    Proc ! {find_successor, {Me, Hash}}.

pot2(0) -> 1;
pot2(N) -> 2*pot2(N-1).

createFingerTable(K,M) when K > M -> ok;
createFingerTable(K,M) ->
    put({finger,K}, #id{}),
    createFingerTable(K+1,M).

get_fingerTable(I,M) when I > M -> [];
get_fingerTable(I,M) -> [{I,get({finger,I})}|get_fingerTable(I+1,M)].
