all: mvt_s
	
mvt_s: mvt_s.c
	gcc -o mvt_s mvt_s.c -lm -ggdb -lrt -lpthread