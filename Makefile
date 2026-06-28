.PHONY: build test package clean

build:
	cargo build --release

test:
	cargo test

package: build
	mkdir -p dist/native
	cp target/release/libirodori_extension_* dist/native/ 2>/dev/null || true
	cp target/release/irodori_extension_*.dll dist/native/ 2>/dev/null || true
	cp target/release/libirodori_extension_*.dylib dist/native/ 2>/dev/null || true

clean:
	cargo clean
