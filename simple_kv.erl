-module(simple_kv).
-export([start_link/2, stop/1, put/3, put/4, delete/2, get/2, enforce_alpha/3]).

-spec start_link(pos_integer(), pos_integer()) -> {ok, [pid()]}.
start_link(Count, Alfa) when Count > 0 ->
    Pids = create_processes(Count, [], Alfa),
    set_neighbor_threads(Pids, Pids),
    {ok, Pids}.

create_processes(0, Pids, _) ->
    Pids;
create_processes(N, Pids, Alfa) ->
    Pid = spawn(fun() ->
            % setteamos un scheduler que "limpie" los valores invalidos de la db
            erlang:send_after(5000, self(), clean_tombstones),
            simple_kv_main:lifecycle(#{}, [], Alfa, 0)
    end),
    create_processes(N - 1, [Pid | Pids], Alfa).

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

enforce_alpha(Database, Alpha, NeighborThreads) ->
    % filtrar valores validos
    ValidEntries = maps:filter(fun
        (_Key, {tombstone, _TS}) -> false;
        (_Key, {_Value, _TS}) -> true
    end, Database),
    
    case map_size(ValidEntries) > Alpha of
        true ->
            [{FirstKey, {_FirstVal, FirstTS}} | Rest] = maps:to_list(ValidEntries),
            {OldestKey, _OldestTS} = lists:foldl(
                fun({K, {_V, TS}}, {MinK, MinTS}) ->
                    if TS < MinTS -> {K, TS};
                       true -> {MinK, MinTS}
                    end
                end, 
                {FirstKey, FirstTS}, 
                Rest
            ),

            % Como removimos un valor, se lo ofrecemos al resto por si lo quiere
            {OldestValue, OldestTS} = maps:get(OldestKey, ValidEntries),
            lists:foreach(fun(NeighborPid) -> 
                if NeighborPid =/= self() ->
                  NeighborPid ! {offer_key_value, OldestKey, OldestValue, OldestTS};
                  true -> ok
                end
            end, NeighborThreads),

            maps:remove(OldestKey, Database);
        false ->
            Database
    end.
