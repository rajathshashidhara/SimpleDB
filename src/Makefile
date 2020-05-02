DIR := $(shell pwd)
DIR_LEVELDB := $(DIR)/../leveldb
DIR_LIBUV := $(DIR)/../libuv

CXX_SRCS := simpledb_main.cpp
CXX_OBJS := $(CXX_SRCS:.cpp=.o)
CXX_DEPS := $(CXX_SRCS:.cpp=.d)

PROTOBUF_LIBS := $(shell pkg-config --libs protobuf)
LIBS_DIR := storage net execution util formats
APP_LIBS := $(addprefix -l, $(LIBS_DIR))
LIBS_INC := $(addprefix -L$(DIR)/, $(LIBS_DIR))

CPPFLAGS := -I$(DIR) \
		-I$(DIR_LEVELDB)/include \
		-I$(DIR_LIBUV)/include \

CXXFLAGS := -g3 -Wall -Werror -MD -MP

LDFLAGS := -L$(DIR_LEVELDB)/build \
			-L$(DIR_LIBUV)/.libs \
			$(LIBS_INC)
LDLIBS := $(APP_LIBS) -luv -lleveldb $(PROTOBUF_LIBS) -lglog

BIN := simpledb.out

PROTODIR := formats

all: $(PROTODIR) $(BIN) lambdalibs

$(BIN): $(LIBS_DIR) $(CXX_OBJS)
	$(CXX) $(LDFLAGS) -o $@ $(CXX_OBJS) $(LDLIBS)

$(LIBS_DIR):
	$(MAKE) -C $@

$(PROTODIR):
	$(MAKE) -C $(PROTODIR)

lambdalibs:
	$(MAKE) -C execution

clean:
	rm -vf $(CXX_OBJS) $(CXX_DEPS) $(BIN)
	$(MAKE) -C execution clean
	for dir in $(LIBS_DIR); do \
		$(MAKE) -C $$dir clean; \
	done

-include $(CXX_DEPS)

.PHONY : all $(PROTODIR) clean $(LIBS_DIR)