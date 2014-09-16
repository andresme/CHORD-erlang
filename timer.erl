%%% File    : timer.erl
%%% Author  : JCastro <>
%%% Description : 
%%% Created :  3 Sep 2014 by JCastro <>

-module(timer).

-export([clock/3]).

clock(Name, TicMin, TicMax) ->
    {A,B,C} = now(),
    random:seed(A,B,C),
    click(Name,TicMin,TicMax).

click(Name,TicMin,TicMax) ->
    N = random:uniform(TicMax-TicMin) + TicMin,
    receive
	stop -> ok
    after N ->
	    case random:uniform(3) of
		1 -> chord:stabilize  (Name);
		2 -> chord:fix_fingers(Name);
		3 -> chord:check_pred (Name)
	    end,
	    click(Name,TicMin,TicMax)
    end.
			

