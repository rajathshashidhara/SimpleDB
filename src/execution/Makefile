
CXXFLAGS := -g3 -O2 -Wall -Werror -MD -MP

LIB := libcpplambda.a

CXX_SRCS := cpplambda.cpp
CXX_OBJS := $(CXX_SRCS:.cpp=.o)
CXX_DEPS := $(CXX_SRCS:.cpp=.d)

CPPFLAGS := -I..

LDFLAGS := -L$(DIR)/formats
LDLIBS :=  -lformats $(PROTOBUF_LIBS)

FORMATS_LIB := libformats.a

all: $(FORMATS_LIB) $(LIB)

$(LIB): $(CXX_OBJS)
	$(AR) rcu $@ $+
	ranlib $@

$(FORMATS_LIB):
	$(MAKE) -C ../formats

clean:
	rm -vf $(CXX_OBJS) $(CXX_DEPS) $(LIB)
	$(MAKE) -C ../formats clean

-include $(CXX_DEPS)