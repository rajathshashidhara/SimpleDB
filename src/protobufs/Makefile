
%.pb.cc: %.proto
	protoc --cpp_out=. $<

%.pb.o: %.pb.cc
	$(CXX) $(CXX_FLAGS) -c -o $@ $<
