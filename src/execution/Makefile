DIR := $(shell pwd)
DIR_LEVELDB := $(DIR)/../../leveldb
DIR_LIBUV := $(DIR)/../../libuv

CPPFLAGS := -I$(DIR) \
			-I$(DIR)/.. \
			-I$(DIR_LIBUV)/include \
			-I$(DIR_LEVELDB)/include \

CXXFLAGS := -g3 -O2 -Wall -Werror -MD -MP

LIB_CPPLAMBDA := libcpplambda.a
LIB_CPPLAMBDA_SRCS := cpplambda.cpp
LIB_CPPLAMBA_OBJS := $(LIB_CPPLAMBDA_SRCS:.cpp=.o)

LIB_EXEC := libexecution.a
LIB_EXEC_SRCS := runtime.cpp
LIB_EXEC_OBJS := $(LIB_EXEC_SRCS:.cpp=.o)

CXX_SRCS := $(LIB_CPPLAMBDA_SRCS) $(LIB_EXEC_SRCS)
CXX_OBJS := $(CXX_SRCS:.cpp=.o)
CXX_DEPS := $(CXX_SRCS:.cpp=.d)

FORMATS_LIB := libformats.a
LIBS := $(LIB_CPPLAMBDA) $(LIB_EXEC)

all: $(FORMATS_LIB) $(LIBS)

$(LIB_CPPLAMBDA): $(LIB_CPPLAMBDA_OBJS)
	$(AR) rcu $@ $+
	ranlib $@

$(LIB_EXEC): $(LIB_EXEC_OBJS)
	$(AR) rcu $@ $+
	ranlib $@

$(FORMATS_LIB):
	$(MAKE) -C ../formats

clean:
	rm -vf $(CXX_OBJS) $(CXX_DEPS) $(LIBS)
	$(MAKE) -C ../formats clean

-include $(CXX_DEPS)