
CXX=g++
CC=cc
AR=ar
CXXFLAGS=-g -Wall -o2 -std=c++0x -DDEBUG
LIBS=/usr/local/lib//libhiredis.a

EXAMPLE=example/simple
UNITTEST=unittest/unittest
STATIC=libredis_cluster.a
TARGETS=$(STATIC) $(UNITTEST) $(EXAMPLE)

all: $(TARGETS) 

lib: $(STATIC)

$(UNITTEST): unittest/unittest.o redis_cluster.o
	$(CXX) $^ -o $@ $(LIBS) /usr/local/lib/libgtest.a

$(EXAMPLE): example/simple.o redis_cluster.o
	$(CXX) $^ -o $@ $(LIBS)

$(STATIC): redis_cluster.o
	$(AR) rc $@ $^

clean:
	rm -rf *.o unittest/*.o example/*.o $(TARGETS)

install:
	cp redis_cluster.hpp /usr/local/include/
	cp $(STATIC) /usr/local/lib/
