DIR := $(shell pwd)
DIR_LEVELDB := $(DIR)/../leveldb
DIR_LIBUV := $(DIR)/../libuv

CXX_SRCS := simpledb_main.cpp \
		db/db.cpp \
		net/service.cpp \
		net/connections.cpp

CXX_OBJS := $(CXX_SRCS:.cpp=.o)
CXX_DEPS := $(CXX_SRCS:.cpp=.d)

PROTOBUF_LIBS := $(shell pkg-config --libs protobuf)

CPPFLAGS := -I$(DIR) \
		-I$(DIR_LEVELDB)/include \
		-I$(DIR_LIBUV)/include \

CXXFLAGS := -O2 -g3 -Wall -Werror -MD -MP

LDFLAGS := -L$(DIR_LEVELDB)/build \
			-L$(DIR_LIBUV)/.libs \
			-L$(DIR)/protobufs
LDLIBS := -luv -lleveldb -lformats $(PROTOBUF_LIBS) -lglog


BIN := simpledb.out

FORMATS_LIB := libformats.a
all: $(FORMATS_LIB) $(BIN) lambdalibs

$(BIN): $(CXX_OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS)

$(FORMATS_LIB):
	$(MAKE) -C protobufs

lambdalibs:
	$(MAKE) -C execution

clean:
	rm -vf $(CXX_OBJS) $(CXX_DEPS) $(BIN)
	$(MAKE) -C protobufs clean
	$(MAKE) -C execution clean

-include $(CXX_DEPS)