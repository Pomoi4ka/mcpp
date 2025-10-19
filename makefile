LIBCO_PATH = libco

main: main.cpp $(LIBCO_PATH)/libco.o
	$(CXX) -Wall -Wextra -std=c++17 -o $@ $< $(LIBCO_PATH)/libco.o -I$(LIBCO_PATH) -lz

$(LIBCO_PATH)/libco.o: $(LIBCO_PATH)/libco.c
	$(CC) -c $< -o $@
