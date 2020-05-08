
CXX_FLAGS := -g3 -O2 -Wall -Werror

LIB := libformats.a

PRSRCS := netformats.proto \
			serialization.proto \
			execformats.proto
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
