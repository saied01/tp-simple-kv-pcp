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
        
        % propagate put
        {propagate, put, Key, Value, TTL} ->
            UpdatedDatabase = maps:put(Key, Value, Database),
            lists:foreach(fun(NeighborPid) -> 
                NeighborPid ! {update_propagated_db, UpdatedDatabase}
            end, NeighborThreads),
            lifecycle(UpdatedDatabase, NeighborThreads);

        %  propagate delete
        {propagate, delete, Key} ->
            UpdatedDatabase = maps:remove(Key, Database),
            lists:foreach(fun(NeighborPid) ->
                NeighborPid ! {update_propagated_db, UpdatedDatabase}
            end, NeighborThreads),
            lifecycle(UpdatedDatabase, NeighborThreads);


        %  propagation
        {update_propagated_db, DB} ->
            lifecycle(DB, NeighborThreads);

        % stop
        {stop, SenderPid} ->
            SenderPid ! {stopped, self()},
            ok
    end.