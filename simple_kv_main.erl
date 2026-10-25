-module(simple_kv_main).
-export([lifecycle/2]).


lifecycle(Database, NeighborThreads) ->
    receive
        % set neighbors
        {set_neighbor_threads, Pids} ->
            lifecycle(Database, Pids);

        % put
        {put, Key, Value, TTL, SenderPid} ->
            UpdatedDatabase = maps:put(Key, Value, Database),
            SenderPid ! ok,
            if TTL =/= infinity ->
              erlang:send_after(TTL, self(), {ttl_expired, Key});
              true -> ok
            end,
            lifecycle(UpdatedDatabase, NeighborThreads);
            
        % get
        {get, Key, SenderPid} ->
            case maps:find(Key, Database) of
                {ok, Value} ->
                    SenderPid ! {ok, Value};
                error ->
                    SenderPid ! not_found
            end,
            lifecycle(Database, NeighborThreads);
            
    
        % delete
        {delete, Key, SenderPid} ->
            UpdatedDatabase = maps:remove(Key, Database),
            SenderPid ! ok,
            lifecycle(UpdatedDatabase, NeighborThreads);

        % delete after ttl expiration
        {ttl_expired, Key} ->
            lists:foreach(fun(NeighborPid) -> 
                NeighborPid ! {silent_delete, Key}
            end, NeighborThreads),
            lifecycle(Database, NeighborThreads);

        % delete for propagation
        {silent_delete, Key} ->
            UpdatedDatabase = maps:remove(Key, Database),
            lifecycle(UpdatedDatabase, NeighborThreads);

        % put for propagation
        {silent_put, Key, Value} -> 
            UpdatedDatabase = maps:put(Key, Value, Database),
            lifecycle(UpdatedDatabase, NeighborThreads);
        
        % propagate put
        {propagate, put, Key, Value} ->
            lists:foreach(fun(NeighborPid) -> 
                if NeighborPid =/= self() ->
                  NeighborPid ! {silent_put, Key, Value};
                  true -> ok
                end
            end, NeighborThreads),
            lifecycle(Database, NeighborThreads);

        %  propagate delete
        {propagate, delete, Key} ->
            lists:foreach(fun(NeighborPid) -> 
                if NeighborPid =/= self() ->
                  NeighborPid ! {silent_delete, Key};
                  true -> ok
                end
            end, NeighborThreads),
            lifecycle(Database, NeighborThreads);


        %  propagation
        {update_propagated_db, DB} ->
            lifecycle(DB, NeighborThreads);

        % stop
        {stop, SenderPid} ->
            SenderPid ! {stopped, self()},
            ok
    end.
