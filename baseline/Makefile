DIR := $(shell pwd)
DIR_LEVELDB := $(DIR)/../leveldb
DIR_LIBUV := $(DIR)/../libuv

CXX_FLAGS := -g3 -O2 -Wall -Werror

PRSRCS := gg.proto
PRCPPS := $(PRSRCS:.proto=.pb.cc)
PRINCS := $(PRSRCS:.proto=.pb.h)
PROBJS := $(PRSRCS:.proto=.pb.o)

CXX_SRCS := base64.cpp \
			ggpaths.cpp \
			execution.cpp \
			baseline_exec.cpp
CXX_OBJS := $(CXX_SRCS:.cpp=.o)
CXX_DEPS := $(CXX_SRCS:.cpp=.d)

PROTOBUF_LIBS := $(shell pkg-config --libs protobuf)

CPPFLAGS := -I$(DIR) \
		-I$(DIR_LIBUV)/include

LDFLAGS := -L$(DIR_LIBUV)/.libs

CXXFLAGS := -g3 -Wall -Werror -MD -MP

LDLIBS := -luv $(PROTOBUF_LIBS) -lglog

BIN := ggengine.out

all: $(BIN)

%.pb.cc: %.proto
	protoc --cpp_out=. $<

%.pb.o: %.pb.cc
	$(CXX) $(CXX_FLAGS) -c -o $@ $<

$(BIN): $(PROBJS) $(CXX_OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS)

clean:
	rm -vf $(CXX_OBJS) $(CXX_DEPS) $(BIN) $(PRCPPS) $(PRINCS) $(PROBJS)

-include $(CXX_DEPS)

