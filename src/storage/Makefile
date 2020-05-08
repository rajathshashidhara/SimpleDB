DIR := $(shell pwd)
DIR_LEVELDB := $(DIR)/../../leveldb
DIR_LIBUV := $(DIR)/../../libuv

CPPFLAGS := -I$(DIR) \
			-I$(DIR)/.. \
			-I$(DIR_LIBUV)/include \
			-I$(DIR_LEVELDB)/include

CXX_SRCS := db.cpp\
			cache.cpp
CXX_OBJS := $(CXX_SRCS:.cpp=.o)
CXX_DEPS := $(CXX_SRCS:.cpp=.d)

CXXFLAGS := -g3 -O2 -Wall -Werror -MD -MP

LIB := libstorage.a

all: $(LIB)

$(LIB): $(CXX_OBJS)
	$(AR) rcu $@ $+
	ranlib $@

clean:
	rm -vf $(CXX_OBJS) $(CXX_DEPS) $(LIB)

-include $(CXX_DEPS)
