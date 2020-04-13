
CXX_FLAGS := -g3 -Wall -Werror

LIB := libformats.a

PRSRCS := $(wildcard *.proto)
PRCPPS := $(PRSRCS:.proto=.pb.cc)
PRINCS := $(PRSRCS:.proto=.pb.h)
PROBJS := $(PRSRCS:.proto=.pb.o)

all: $(LIB)

%.pb.cc: %.proto
	protoc --cpp_out=. $<

%.pb.o: %.pb.cc
	$(CXX) $(CXX_FLAGS) -c -o $@ $<

$(LIB): $(PROBJS)
	$(AR) rcu $@ $+
	ranlib $@

clean:
	rm -rf $(PRCPPS) $(PRINCS) $(PROBJS) $(LIB)