-module(simple_kv_main).
-export([lifecycle/2]).


lifecycle(Database, NeighborThreads) ->
    receive
        % set neighbors
        {set_neighbor_threads, Pids} ->
            lifecycle(Database, Pids);

        % put
        {put, Key, Value, TTL, SenderPid} ->
            Timestamp = {erlang:system_time(microsecond), self()},
            UpdatedDatabase = maps:put(Key, {Value, Timestamp}, Database),
            SenderPid ! ok,

            if TTL =/= infinity ->
                erlang:send_after(TTL, self(), {ttl_expired, Key});
               true -> ok
            end,

          % propagate put
            lists:foreach(fun(NeighborPid) -> 
                if NeighborPid =/= self() ->
                  NeighborPid ! {silent_put, Key, Value, Timestamp};
                  true -> ok
                end
            end, NeighborThreads),

            lifecycle(UpdatedDatabase, NeighborThreads);
            
        % get
        {get, Key, SenderPid} ->
            case maps:find(Key, Database) of
                {ok, {tombstone, _}} ->
                    SenderPid ! not_found;
                {ok, {Value, _}} ->
                    SenderPid ! {ok, Value};
                error ->
                    SenderPid ! not_found
            end,

            lifecycle(Database, NeighborThreads);
            
    
        % delete
        {delete, Key, SenderPid} ->
            Timestamp = {erlang:system_time(microsecond), self()},
            UpdatedDatabase = maps:put(Key, {tombstone, Timestamp}, Database),
            SenderPid ! ok,

            %  propagate delete
            lists:foreach(fun(NeighborPid) -> 
                if NeighborPid =/= self() ->
                  NeighborPid ! {silent_delete, Key, Timestamp};
                  true -> ok
                end
            end, NeighborThreads),

            lifecycle(UpdatedDatabase, NeighborThreads);

        % delete after ttl expiration
        {ttl_expired, Key} ->
            Timestamp = {erlang:system_time(microsecond), self()},
            UpdatedDatabase = maps:put(Key, {tombstone, Timestamp}, Database),

            % propagate like delete
            lists:foreach(fun(NeighborPid) -> 
                NeighborPid ! {silent_delete, Key, Timestamp}
            end, NeighborThreads),

            lifecycle(UpdatedDatabase, NeighborThreads);

        % delete for propagation
        {silent_delete, Key, ComingTimestamp} ->
            UpdatedDatabase = case maps:find(Key, Database) of
                {ok, {_OldValue, CurrTimestamp}} when ComingTimestamp < CurrTimestamp ->
                      Database;
                _ ->
                      maps:put(Key, {tombstone, ComingTimestamp}, Database)
              end,

            lifecycle(UpdatedDatabase, NeighborThreads);

        % put for propagation
        {silent_put, Key, Value, ComingTimestamp} -> 
            UpdatedDatabase = case maps:find(Key, Database) of
                {ok, {_OldValue, Timestamp}} when ComingTimestamp < Timestamp ->
                  Database;
                _ ->
                  maps:put(Key, {Value, ComingTimestamp}, Database)
            end,

            lifecycle(UpdatedDatabase, NeighborThreads);

        % stop
        {stop, SenderPid} ->
            SenderPid ! {stopped, self()},
            ok
    end.
