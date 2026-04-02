EXTENSION = pacman_agent
MODULE_big = pacman_agent
OBJS = pacman_agent.o
DATA = sql/pacman_agent--0.1.0.sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

ifeq ($(PGXS),)
$(error pg_config was not found; set PG_CONFIG to a PostgreSQL 17 installation)
endif

PG_CPPFLAGS += -DPACMAN_AGENT_VERSION=\"0.1.0\"

include $(PGXS)
