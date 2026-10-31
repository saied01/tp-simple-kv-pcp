-module(simple_kv_tests).
% Incluimos la librería de testing de Erlang
-include_lib("eunit/include/eunit.hrl").

% TEST 1: operaciones secuenciales normales y error por clausula
basic_operations_test() ->
    {ok, [P1, P2] = Pids} = simple_kv:start_link(2, 50),
    
    ?assertEqual(ok, simple_kv:put(P1, color, rojo)),
    
    timer:sleep(10),
    
    ?assertEqual({ok, rojo}, simple_kv:get(P2, color)),
    
    ?assertEqual(ok, simple_kv:delete(P1, color)),
    
    timer:sleep(10),
    
    ?assertEqual(not_found, simple_kv:get(P1, color)),
    
    ?assertError(function_clause, simple_kv:put(P1, x, 1, -5)),
    
    ?assertEqual(ok, simple_kv:stop(Pids)).

% TEST 2: Operaciones Concurrentes
concurrent_writes_test() ->
    {ok, [P1, P2, P3] = Pids} = simple_kv:start_link(3, 50),
    TestPid = self(), % Guardamos el PID del test para recibir confirmaciones
    
    spawn(fun() -> 
        simple_kv:put(P1, color, rojo),
        TestPid ! {done, rojo}
    end),
    
    spawn(fun() -> 
        simple_kv:put(P2, color, amarillo),
        TestPid ! {done, amarillo}
    end),
    
    spawn(fun() -> 
        simple_kv:put(P3, color, azul),
        TestPid ! {done, azul}
    end),

    receive {done, _} -> ok end,
    receive {done, _} -> ok end,
    receive {done, _} -> ok end,

    timer:sleep(50),

    % Todos los valores deberian ser iguales si no hubo ninguna race condition
    {ok, ValorP1} = simple_kv:get(P1, color),
    {ok, ValorP2} = simple_kv:get(P2, color),
    {ok, ValorP3} = simple_kv:get(P3, color),

    ?assertEqual(ValorP1, ValorP2),
    ?assertEqual(ValorP2, ValorP3),

    % Por si lo quiero imprimir
    % io:format(user, "~nResultado concurrente P1: ~p~n", [ValorP1]),
    % io:format(user, "Resultado concurrente P2: ~p~n", [ValorP2]),
    % io:format(user, "Resultado concurrente P3: ~p~n", [ValorP3]),

    simple_kv:stop(Pids).

% TEST 3: prevencion de escritura 'zombie' retrasada (test de caja blanca, porque sino no se como hacerlo)
zombie_data_prevention_test() ->
    {ok, [P]} = simple_kv:start_link(1, 50),
    Key = color,
    OldTS = {100, self()},
    DeleteTS = {101, self()},

    P ! {silent_put, Key, rojo, OldTS},
    ?assertEqual({ok, rojo}, simple_kv:get(P, Key)),

    P ! {silent_delete, Key, DeleteTS},
    ?assertEqual(not_found, simple_kv:get(P, Key)),
    
    % Reentrega retrasada de una escritura anterior.
    P ! {silent_put, Key, rojo, OldTS},
    ?assertEqual(not_found, simple_kv:get(P, Key)),

    % Una oferta vieja tampoco debe resucitar la clave.
    P ! {offer_key_value, Key, rojo, OldTS},
    ?assertEqual(not_found, simple_kv:get(P, Key)),

    simple_kv:stop([P]).

% TEST 4: borrado de valor por exceso de espacio (alfa)
alpha_deletion_and_test() ->
    {ok, [P1, P2]} = simple_kv:start_link(2, 2),
    simple_kv:put(P1, k1, v1),
    simple_kv:put(P1, k2, v2),

    timer:sleep(50),

    simple_kv:put(P1, k3, v3),

    timer:sleep(100),

    ?assertEqual(not_found, simple_kv:get(P1, k1)),
    ?assertEqual({ok, v1}, simple_kv:get(P2, k1)),

    simple_kv:stop([P1, P2]).
