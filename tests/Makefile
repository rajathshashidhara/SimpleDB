DIR := $(shell pwd)
DIR_SERVER := $(DIR)/../src
DIR_LEVELDB := $(DIR)/../leveldb
DIR_LIBUV := $(DIR)/../libuv

CXX_SRCS := client.cpp

CXX_OBJS := $(CXX_SRCS:.cpp=.o)
CXX_DEPS := $(CXX_SRCS:.cpp=.d)

PROTOBUF_LIBS := $(shell pkg-config --libs protobuf)

CPPFLAGS := -I$(DIR_SERVER) \

CXXFLAGS := -g3 -Wall -Werror -MD -MP

LDFLAGS := -L$(DIR_SERVER)/protobufs
LDLIBS := -lformats $(PROTOBUF_LIBS)


BIN := testclient.out

FORMATS_LIB := libformats.a

all: $(FORMATS_LIB) $(BIN)

$(BIN): $(CXX_OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS)

$(FORMATS_LIB):
	$(MAKE) -C $(DIR_SERVER)/protobufs

clean:
	rm -vf $(CXX_OBJS) $(CXX_DEPS) $(BIN)
	$(MAKE) -C $(DIR_SERVER)/protobufs clean

-include $(CXX_DEPS)