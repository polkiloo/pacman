.PHONY: build build-pacmand build-pacmanctl build-pg-extension package-pg-extension install-pg-extension clean-pg-extension clean

build: build-pacmand build-pacmanctl
# The PostgreSQL extension is built inside a Docker container.
# See postgresql/pacman_agent/Makefile.docker. Run: make build-pg-extension

PG_EXTENSION_MAKE = $(MAKE) -C $(PG_EXTENSION_DIR) -f Makefile.docker \
	PG_EXTENSION_IMAGE="$(PG_EXTENSION_IMAGE)" \
	PG_CONFIG="$(PG_CONFIG)" \
	OUTPUT_DIR="$(abspath $(PG_EXTENSION_OUTPUT))" \
	PACMAND_BIN="$(abspath $(BIN_DIR)/pacmand)"

build-pg-extension:
	$(PG_EXTENSION_MAKE) build

package-pg-extension: build-pg-extension build-pacmand
	$(PG_EXTENSION_MAKE) package

install-pg-extension: package-pg-extension
	$(PG_EXTENSION_MAKE) install

clean-pg-extension:
	$(PG_EXTENSION_MAKE) clean

build-pacmand:
	mkdir -p $(BIN_DIR)
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/pacmand ./cmd/pacmand

build-pacmanctl:
	mkdir -p $(BIN_DIR)
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/pacmanctl ./cmd/pacmanctl

clean:
	rm -rf $(BIN_DIR)
