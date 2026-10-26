.PHONY: all compile test clean

# Comportamiento por defecto al escribir solo 'make'
all: test clean

compile:
	@echo "==> Compilando archivos .erl..."
	erlc *.erl

test: compile
	@echo "==> Ejecutando tests..."
	erl -noshell -eval 'eunit:test(simple_kv_tests, [verbose]), init:stop().'

clean:
	@echo "==> Limpiando archivos .beam..."
	rm -f *.beam
