-module(simple_kv).
-export([start_link/2, stop/1, put/3, put/4, delete/2, get/2]).

-spec start_link(pos_integer(), term()) -> {ok, [pid()]}.
start_link(Count, Alfa) when Count > 0 ->
    Pids = create_processes(Count, []),
    set_neighbor_threads(Pids, Pids),
    % FALTA VERIFICAR ALGO CON EL ALFA NO SE QUE
    {ok, Pids}.

create_processes(0, Pids) ->
    Pids;
create_processes(N, Pids) ->
    Pid = spawn(fun() -> simple_kv_main:lifecycle(#{}, []) end),
    create_processes(N - 1, [Pid | Pids]).

set_neighbor_threads([], _AllPids) ->
    ok;
set_neighbor_threads([Pid | Rest], AllPids) ->
    Pid ! {set_neighbor_threads, AllPids},
    set_neighbor_threads(Rest, AllPids).

-spec stop([pid()]) -> ok.
stop(Pids) ->
    send_stop(Pids),
    wait_stop(Pids).

send_stop([]) -> 
    ok;
send_stop([Pid | Rest]) ->
    Pid ! {stop, self()},
    send_stop(Rest).

wait_stop([]) -> 
    ok;
wait_stop([Pid | Rest]) ->
    receive
        {stopped, Pid} -> 
            wait_stop(Rest)
    after 1000 ->
          wait_stop(Rest)
    end.

-spec put(pid(), term(), term()) -> ok.
put(Pid, Key, Value) ->
    put(Pid, Key, Value, infinity).

-spec put(pid(), term(), term(), non_neg_integer() | infinity) -> ok.
put(Pid, Key, Value, TTL) when (is_integer(TTL) andalso TTL >= 0) orelse TTL =:= infinity ->
    Pid ! {put, Key, Value, TTL, self()},
    propagate_put(Pid, Key, Value),
    receive
        ok -> ok
    end.

-spec delete(pid(), term()) -> ok.
delete(Pid, Key) ->
    Pid ! {delete, Key, self()},
    propagate_delete(Pid ,Key),
    receive
        ok -> ok
    end.

propagate_delete(Pid, Key) ->
    Pid ! {propagate, delete, Key}.

propagate_put(Pid, Key, Value) ->
    Pid ! {propagate, put, Key, Value}.

-spec get(pid(), term()) -> {ok, term()} | not_found.
get(Pid, Key) ->
    Pid ! {get, Key, self()},
    receive
        {ok, Value} -> {ok, Value};
        not_found -> not_found
    end.
