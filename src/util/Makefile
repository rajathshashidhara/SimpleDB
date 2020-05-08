DIR := $(shell pwd)
DIR_LIBUV := $(DIR)/../../libuv

CPPFLAGS := -I$(DIR) \
			-I$(DIR)/.. \
			-I$(DIR_LIBUV)/include

CXX_SRCS := crc16.cpp \
			strict_conversions.cpp \
			file_descriptor.cpp \
			temp_file.cpp \
			path.cpp
CXX_OBJS := $(CXX_SRCS:.cpp=.o)
CXX_DEPS := $(CXX_SRCS:.cpp=.d)

CXXFLAGS := -g3 -O2 -Wall -Werror -MD -MP

LIB := libutil.a

all: $(LIB)

$(LIB): $(CXX_OBJS)
	$(AR) rcu $@ $+
	ranlib $@

clean:
	rm -vf $(CXX_OBJS) $(CXX_DEPS) $(LIB)

-include $(CXX_DEPS)
