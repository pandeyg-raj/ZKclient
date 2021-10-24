all:
	g++ zk_client.cpp -std=c++11 libzookeeper_mt.a -lpthread
debug:
	g++ zk_client.cpp -g -O0 -std=c++11 libzookeeper_mt.a -lpthread
clean:
	rm results/*.txt a.out