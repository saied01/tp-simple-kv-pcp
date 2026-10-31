-module(simple_kv).
-export([start_link/2, stop/1, put/3, put/4, delete/2, get/2]).

-spec start_link(pos_integer(), pos_integer()) -> {ok, [pid()]}.
start_link(Count, Alfa) when Count > 0 ->
    Pids = utils:create_processes(Count, [], Alfa),
    utils:set_neighbor_threads(Pids, Pids),
    {ok, Pids}.


-spec stop([pid()]) -> ok.
stop(Pids) ->
    utils:send_stop(Pids),
    utils:wait_stop(Pids).


-spec put(pid(), term(), term()) -> ok.
put(Pid, Key, Value) ->
    put(Pid, Key, Value, infinity).

-spec put(pid(), term(), term(), non_neg_integer() | infinity) -> ok.
put(Pid, Key, Value, TTL) when (is_integer(TTL) andalso TTL >= 0) orelse TTL =:= infinity ->
    Pid ! {put, Key, Value, TTL, self()},
    receive
        ok -> ok
    end.


-spec delete(pid(), term()) -> ok.
delete(Pid, Key) ->
    Pid ! {delete, Key, self()},
    receive
        ok -> ok
    end.


-spec get(pid(), term()) -> {ok, term()} | not_found.
get(Pid, Key) ->
    Pid ! {get, Key, self()},
    receive
        {ok, Value} -> {ok, Value};
        not_found -> not_found
    end.
