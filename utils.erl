-module(utils).
-export([send_stop/1, wait_stop/1, create_processes/3, set_neighbor_threads/2, enforce_alpha/3, query_neighbors_for_get/2]).


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

create_processes(0, Pids, _) ->
    Pids;
create_processes(N, Pids, Alfa) ->
    Pid = spawn(fun() ->
            simple_kv_main:lifecycle(#{}, [], Alfa, 0)
    end),
    create_processes(N - 1, [Pid | Pids], Alfa).

set_neighbor_threads([], _AllPids) ->
    ok;
set_neighbor_threads([Pid | Rest], AllPids) ->
    Pid ! {set_neighbor_threads, AllPids},
    set_neighbor_threads(Rest, AllPids).

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


query_neighbors_for_get(Key, Neighbors) ->
    % Enviar un mensaje de get a todos los vecinos
    lists:foreach(fun(Pid) -> 
        Pid ! {get_no_propagate, Key, self()} 
    end, Neighbors),
    
    % Recolectar respuestas de los vecinos
    collect_responses(length(Neighbors)).

collect_responses(0) ->
    not_found;
collect_responses(N) ->
    receive
        {ok, Value} ->
            {ok, Value};
        not_found ->
            collect_responses(N - 1)
    after 50 -> % Timeout de seguridad para no bloquear el nodo indefinidamente
        collect_responses(N - 1)
    end.
