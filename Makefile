
CXX=g++
CC=cc
AR=ar

CXXFLAGS=-g -Wall -o2 -std=c++0x
LIBS=/usr/local/lib/libgtest.a /usr/local/lib//libhiredis.a

UNITTEST=unittest/unittest
STATIC=libredis_cluster.a
TARGETS=$(STATIC) $(UNITTEST)

all: $(TARGETS) 

$(UNITTEST): unittest/unittest.o redis_cluster.o
	$(CXX) $^ -o $@ $(LIBS)

$(STATIC): redis_cluster.o
	$(AR) rc $@ $^

clean:
	rm -rf *.o unittest/*.o $(TARGETS)