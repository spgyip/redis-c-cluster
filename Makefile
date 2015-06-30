
CXX=g++
CC=cc
AR=ar

CXXFLAGS=-g -Wall -o2 -std=c++0x
LIBS=/usr/local/lib//libhiredis.a

TEST=test/test
EXAMPLE=example/simple
UNITTEST=unittest/unittest
STATIC=libredis_cluster.a
TARGETS=$(STATIC) $(UNITTEST) $(TEST) $(EXAMPLE)

all: $(TARGETS) 

lib: $(STATIC)

$(UNITTEST): unittest/unittest.o redis_cluster.o
	$(CXX) $^ -o $@ $(LIBS) /usr/local/lib/libgtest.a

$(TEST): test/test.o redis_cluster.o
	$(CXX) $^ -o $@ $(LIBS)

$(EXAMPLE): example/simple.o redis_cluster.o
	$(CXX) $^ -o $@ $(LIBS)

$(STATIC): redis_cluster.o
	$(AR) rc $@ $^

clean:
	rm -rf *.o unittest/*.o test/*.o example/*.o $(TARGETS)
