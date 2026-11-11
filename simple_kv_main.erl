-module(simple_kv_main).
-export([lifecycle/4]).

lifecycle(Database, NeighborThreads, Alfa, ValidSize) ->
    receive
        % set neighbors
        {set_neighbor_threads, Pids} ->
            erlang:send_after(60000, self(), tombstone_cleanup),
            lifecycle(Database, Pids, Alfa, ValidSize);

        % put
        {put, Key, Value, TTL, SenderPid, Ref} ->
            Timestamp = {erlang:system_time(microsecond), self()},
            
            UpdatedDatabase = maps:put(Key, {Value, Timestamp}, Database),
            SenderPid ! {Ref, ok},

            if TTL =/= infinity ->
                erlang:send_after(TTL, self(), {ttl_expired, Key, Timestamp});
               true -> ok
            end,

            % propagate put
            lists:foreach(fun(NeighborPid) -> 
                if NeighborPid =/= self() ->
                    NeighborPid ! {silent_put, Key, Value, Timestamp};
                   true -> ok
                end
            end, NeighborThreads),

            % Checkea que el tamanio no sea mayor a Alfa y aplica enforce_alpha
            FinalDatabase = utils:enforce_alpha(UpdatedDatabase, Alfa, NeighborThreads),
            
            % Recalculamos el tamaño de válidos limpio tras el enforce_alpha
            ValidEntries = maps:filter(fun(_K, {tombstone, _TS}) -> false; (_K, {_V, _TS}) -> true end, FinalDatabase),
            FinalSize = map_size(ValidEntries),

            lifecycle(FinalDatabase, NeighborThreads, Alfa, FinalSize);
            
        % get
        {get, Key, SenderPid, Ref} ->
            Result = case maps:find(Key, Database) of
                {ok, {tombstone, _}} ->
                    not_found;
                {ok, {Value, _}} ->
                    {ok, Value};
                error ->
                    utils:query_neighbors_for_get(Key, NeighborThreads, Ref)
            end,

            SenderPid ! {Ref, Result},

            lifecycle(Database, NeighborThreads, Alfa, ValidSize);

        % el mismo get solo que no propaga a otras replicas
        {get_no_propagate, Key, SenderPid, QueryRef} ->
            Result = case maps:find(Key, Database) of
                {ok, {tombstone, _}} ->
                         not_found;
                {ok, {Value, _}} ->
                         {ok, Value};
                error ->
                         not_found
            end,

            SenderPid ! {QueryRef, Result},

            lifecycle(Database, NeighborThreads, Alfa, ValidSize);
    
        % delete
        {delete, Key, SenderPid, Ref} ->
            Timestamp = {erlang:system_time(microsecond), self()},
            
            WasValidValue = case maps:get(Key, Database, not_found) of
                {tombstone, _} -> false;
                not_found -> false;
                _ -> true
            end,
            
            UpdatedDatabase = if 
                WasValidValue -> maps:put(Key, {tombstone, Timestamp}, Database); 
                true -> Database 
            end,
            NewSize = if WasValidValue -> ValidSize - 1; true -> ValidSize end,
            
            SenderPid ! {Ref, ok},

            % propagate delete
            lists:foreach(fun(NeighborPid) -> 
                if NeighborPid =/= self() ->
                    NeighborPid ! {silent_delete, Key, Timestamp};
                   true -> ok
                end
            end, NeighborThreads),

            lifecycle(UpdatedDatabase, NeighborThreads, Alfa, NewSize);

        % delete after ttl expiration
        {ttl_expired, Key, TimerTimestamp} ->
            {UpdatedDatabase, NewSize} = case maps:find(Key, Database) of
                {ok, {_Val, TimerTimestamp}} ->
                    WasValidValue = case maps:get(Key, Database, not_found) of
                        {tombstone, _} -> false;
                        not_found -> false;
                        _ -> true
                    end,
                    
                    Timestamp = {erlang:system_time(microsecond), self()},
                    NewDb = maps:put(Key, {tombstone, Timestamp}, Database),
                    Size = if WasValidValue -> ValidSize - 1; true -> ValidSize end,

                    % Propagamos el borrado
                    lists:foreach(fun(NeighborPid) -> 
                        if NeighborPid =/= self() ->
                            NeighborPid ! {silent_delete, Key, Timestamp};
                           true -> ok
                        end
                    end, NeighborThreads),

                    {NewDb, Size};

                _ ->
                  % If Timestamp does not match, then it ignores TTL
                    {Database, ValidSize}
            end,

            lifecycle(UpdatedDatabase, NeighborThreads, Alfa, NewSize);
        % Limpieza periódica de tombstones antiguas
        % si la tombstone es muy reciente no se borra por seguridad
        tombstone_cleanup ->
            Now = erlang:system_time(microsecond),
            GracePeriod = 60000 * 1000,

            CleanedDatabase = maps:filter(fun(_Key, Entry) ->
                case Entry of
                    {tombstone, {TSTime, _Pid}} ->
                        (Now - TSTime) < GracePeriod;
                    _ ->
                        true
                end
            end, Database),

            erlang:send_after(60000, self(), tombstone_cleanup),

            lifecycle(CleanedDatabase, NeighborThreads, Alfa, ValidSize);

        % cuando un valor es borrado por alfa, se ofrece al resto, este lo acepta si puede y si no lo tiene ya
        {offer_key_value, Key, OfferedValue, OfferedTS} ->
            case maps:find(Key, Database) of
                {ok, {_Val, CurrTS}} when OfferedTS =< CurrTS ->
                    lifecycle(Database, NeighborThreads, Alfa, ValidSize);
                {ok, {tombstone, CurrTS}} when OfferedTS =< CurrTS ->
                    lifecycle(Database, NeighborThreads, Alfa, ValidSize);
                _ ->
                    case ValidSize >= Alfa of
                        true ->
                            lifecycle(Database, NeighborThreads, Alfa, ValidSize);
                            
                        false ->
                            
                            UpdatedDatabase = maps:put(Key, {OfferedValue, OfferedTS}, Database),
                            ValidEntries = maps:filter(fun(_K, {tombstone, _TS}) -> false; (_K, {_V, _TS}) -> true end, UpdatedDatabase),
                            NewSize = map_size(ValidEntries),
                            
                            lifecycle(UpdatedDatabase, NeighborThreads, Alfa, NewSize)
                    end
            end;

        % delete for propagation
        {silent_delete, Key, ComingTimestamp} ->
            UpdatedDatabase = case maps:find(Key, Database) of
                {ok, {_OldValue, CurrTimestamp}} when ComingTimestamp < CurrTimestamp ->
                    Database;
                _ ->
                    maps:put(Key, {tombstone, ComingTimestamp}, Database)
            end,

            % Recalculamos el tamaño limpio de forma automática (igual que en put y silent_put)
            ValidEntries = maps:filter(fun(_K, {tombstone, _TS}) -> false; (_K, {_V, _TS}) -> true end, UpdatedDatabase),
            NewSize = map_size(ValidEntries),

            lifecycle(UpdatedDatabase, NeighborThreads, Alfa, NewSize);

        % put for propagation
        {silent_put, Key, Value, ComingTimestamp} -> 
            UpdatedDatabase = case maps:find(Key, Database) of
                {ok, {_OldValue, Timestamp}} when ComingTimestamp < Timestamp ->
                    Database;
                {ok, {tombstone, TombTimestamp}} when ComingTimestamp =< TombTimestamp ->
                    Database;
                _ ->
                    maps:put(Key, {Value, ComingTimestamp}, Database)
            end,

            ValidEntries = maps:filter(fun(_K, {tombstone, _TS}) -> false; (_K, {_V, _TS}) -> true end, UpdatedDatabase),
            NewSize = map_size(ValidEntries),

            lifecycle(UpdatedDatabase, NeighborThreads, Alfa, NewSize);

        % stop
        {stop, SenderPid} ->
            SenderPid ! {stopped, self()},
            ok
    end.
