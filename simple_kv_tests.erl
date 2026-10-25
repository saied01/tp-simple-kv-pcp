-module(simple_kv_tests).
% Incluimos la librería de testing de Erlang
-include_lib("eunit/include/eunit.hrl").

% ==========================================
% TEST 1: Operaciones Secuenciales y Errores
% ==========================================
% En EUnit, cualquier función que termine en "_test" se ejecuta automáticamente.
basic_operations_test() ->
    % 1. Iniciamos los nodos
    {ok, [P1, P2, P3] = Pids} = simple_kv:start_link(3, 50),
    
    % 2. Put en P1
    ?assertEqual(ok, simple_kv:put(P1, color, rojo)),
    
    % Esperamos unos milisegundos para dar tiempo a la propagación asíncrona
    timer:sleep(10),
    
    % 3. Get en P2 (Debería tener el valor replicado)
    ?assertEqual({ok, rojo}, simple_kv:get(P2, color)),
    
    % 4. Delete en P1
    ?assertEqual(ok, simple_kv:delete(P1, color)),
    
    % Damos tiempo a la propagación del borrado
    timer:sleep(10),
    
    % 5. Get en P1 (Debería haber desaparecido)
    ?assertEqual(not_found, simple_kv:get(P1, color)),
    
    % 6. Probar que el TTL negativo genera un error (Crash)
    % ?assertError captura el crash esperado para que el test no falle
    ?assertError(function_clause, simple_kv:put(P1, x, 1, -5)),
    
    % 7. Detenemos los procesos
    ?assertEqual(ok, simple_kv:stop(Pids)).

% ==========================================
% TEST 2: Operaciones Concurrentes
% ==========================================
concurrent_writes_test() ->
    {ok, [P1, P2, P3] = Pids} = simple_kv:start_link(3, 50),
    TestPid = self(), % Guardamos el PID del test para recibir confirmaciones
    
    % Generamos 3 hilos paralelos (clientes) escribiendo al mismo tiempo
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

    % Sincronizamos: Esperamos a que los 3 hilos terminen de enviar sus puts
    receive {done, _} -> ok end,
    receive {done, _} -> ok end,
    receive {done, _} -> ok end,

    % Damos tiempo para que todos los nodos terminen de cruzarse los mensajes de propagación
    timer:sleep(50),

    % Leemos cómo quedó el estado en cada nodo.
    % Como bien dijiste, el resultado dependerá de qué mensaje de propagación llegó último a cada buzón.
    {ok, ValorP1} = simple_kv:get(P1, color),
    {ok, ValorP2} = simple_kv:get(P2, color),
    {ok, ValorP3} = simple_kv:get(P3, color),

    % Imprimimos en consola los resultados para analizarlos
    ?debugFmt("~nResultado concurrente P1: ~p", [ValorP1]),
    ?debugFmt("Resultado concurrente P2: ~p", [ValorP2]),
    ?debugFmt("Resultado concurrente P3: ~p", [ValorP3]),

    simple_kv:stop(Pids).
