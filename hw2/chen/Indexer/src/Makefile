parser: parse.cpp
	g++ -std=c++11 -O3 -o parser parse.cpp

merger: phase1merge.cpp phase2merge.cpp
	g++ -std=c++11 -O3 -o phase1merger phase1merge.cpp
	g++ -std=c++11 -O3 -o phase2merger phase2merge.cpp

compresser: compress.cpp
	g++ -std=c++11 -O3 -o compresser compress.cpp

clean:
	rm parser
	rm phase1merger
	rm phase2merger
	rm compresser
