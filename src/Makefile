all: clean compile

compile:
	g++ zk_client.cpp -std=c++11 liblib_g2logger.a libzookeeper_mt.a -lpthread

clean:
	rm -f results/*.txt a.out *.log

debug:
	g++ zk_client.cpp -g -O0 -std=c++11 liblib_g2logger.a libzookeeper_mt.a -lpthread
