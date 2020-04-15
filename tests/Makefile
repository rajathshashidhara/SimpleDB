DIR := $(shell pwd)
DIR_SERVER := $(DIR)/../src
DIR_LEVELDB := $(DIR)/../leveldb
DIR_LIBUV := $(DIR)/../libuv

CXX_SRCS := test_ops.cpp \
			test_integrity.cpp

CXX_OBJS := $(CXX_SRCS:.cpp=.o)
CXX_DEPS := $(CXX_SRCS:.cpp=.d)

PROTOBUF_LIBS := $(shell pkg-config --libs protobuf)

CPPFLAGS := -I$(DIR_SERVER) \

CXXFLAGS := -g3 -Wall -Werror -MD -MP

LDFLAGS := -L$(DIR_SERVER)/protobufs
LDLIBS := -lformats $(PROTOBUF_LIBS)


BINS := $(CXX_SRCS:.cpp=.out)

FORMATS_LIB := libformats.a

all: $(FORMATS_LIB) $(BINS)

%.out: %.o
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS)

$(FORMATS_LIB):
	$(MAKE) -C $(DIR_SERVER)/protobufs

clean:
	rm -vf $(CXX_OBJS) $(CXX_DEPS) $(BINS)
	$(MAKE) -C $(DIR_SERVER)/protobufs clean

-include $(CXX_DEPS)