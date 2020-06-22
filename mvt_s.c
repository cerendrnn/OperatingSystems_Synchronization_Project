#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <stdbool.h>
#include <pthread.h>
#include <semaphore.h>
#define MAX 40


struct arg_struct {
    char* arg1;//filename
    int arg2;//denotes which mapper executes now
    int arg3;//size of splitfile
    int arg4;//currentIndex in buffer of mapper
    int arg5;// # of empty places in buffer of mapper
    int arg6;// # of occupied places in buffer of mapper
    int arg7;//isCompleted
};

struct timeval begin, end;
long time_elapsed = 0;
int* vectorElements;
int** bufferArray;
int* currentIndex;
int* emptyPlaces;
int* occupiedPlaces;
int maxSize;
int vectorSize = 0;
int numMappers;
bool* isAllRead;
int* resultRows;
sem_t* empty;
sem_t* full;
sem_t* mapperSem;

bool readCompleted()
{
    for(int i=0; i<numMappers; i++)
    {
       if(isAllRead[i] == false)
          return false;

    }
    return true;
}

int ceiling(int num1, int num2)
{
    return num1 / num2;
}


int calculateLines(FILE* fileName)
{
    /* calculate the number of lines in input file */
   int nLines = 0;
   //FILE *fp;
   //fp = fopen("m.txt", "r");
   char chr = getc(fileName);

   while (chr != EOF)
   {
        if (chr == '\n')
        {
            nLines++;
        }
        //take next character from file.
        chr = getc(fileName);
   }
   fclose(fileName);
   return nLines;
}

void *processAndMultiply(void *arg)
{
   char* resultfile = ((char *)arg);

     for(int i = 0; i<numMappers; i++)
     {

        sem_wait(&mapperSem[i]);
        if(isAllRead[i] == false)
        {
                    //Now get row, column and mij values from the buffer
          int cur = currentIndex[i];
          int mij = bufferArray[i][cur-1];
          currentIndex[i]--;
          bufferArray[cur-1] = 0;
          int col = bufferArray[i][cur-2];
          currentIndex[i]--;
          bufferArray[cur-2] = 0;
          int row = bufferArray[i][cur-3];
          currentIndex[i]--;
          bufferArray[cur-3] = 0;
          occupiedPlaces[i] = occupiedPlaces[i]-3;
          emptyPlaces[i] = emptyPlaces[i] + 3;

          //Next step is to multiply
          resultRows[row-1] = vectorElements[col-1] * mij + resultRows[row-1];
          sem_post(&mapperSem[i]);
          sem_post(&empty[i]);

        }

   }


    FILE *f = fopen(resultfile, "w");
	if (f == NULL) {
	    printf("Error while opening file!\n");
	    exit(1);
	}

	for (int i = 0; i < vectorSize; ++i)//write the results
    {

		fprintf(f, "%d %d\n",i+1,resultRows[i]);
    }
    fclose(f);
    return 0;
}

void *mapReadPass(void *arg)
{

   struct arg_struct *args = arg;
   char* mapper_filename = args->arg1;
   int size = args->arg3;
   int index = args->arg2;
   FILE* fp =fopen(mapper_filename,"r");

   printf("mapper is reading and passing\n");


   for (int k = 0; k < size; k++)
   {
      sem_wait(&empty[index]);
      sem_wait(&mapperSem[index]);//its mapper's turn
      for(int b=0; b<3; b++)
      {
         fscanf(fp, "%d", &bufferArray[index][currentIndex[index]]);
         printf("\n");
         currentIndex[index]++;
         emptyPlaces[index]--;
         occupiedPlaces[index]++;
         fseek(fp, 1, SEEK_CUR);//omit white spaces between row, column and mij
         if(b==2)
         {
           sem_post(&mapperSem[index]);
           sem_post(&full[index]);
         }
      }

   }
   fclose(fp);//mij values are obtained
   isAllRead[index] = true;
   return 0;

}

int main(int argc, char *argv[])
{
    printf ("mv started\n");
    //argc corresponds to argument count, argv corresponds to argument vector
    //elements of argv will be passed by the program
    int k = atoi(argv[4]);//for partitioning
    //int k=2;
    int bufferSize = atoi(argv[5]);
    //int bufferSize = 10;
    maxSize = bufferSize;
    numMappers = k;
    mapperSem = (sem_t*)malloc(sizeof(sem_t)*k);
    empty = (sem_t*)malloc(sizeof(sem_t)*k);
    full =(sem_t*)malloc(sizeof(sem_t)*k);
    bufferArray = (int**)malloc(sizeof(int)*k);
    currentIndex = (int*)malloc(sizeof(int)*k);
    emptyPlaces = (int*)malloc(sizeof(int)*k);
    occupiedPlaces = (int*)malloc(sizeof(int)*k);
    isAllRead = (bool*)malloc(sizeof(bool)*k);
    int sizeArray[k]; // this array holds the size of every split
    FILE* splitFiles[k];
    char fileoutputname[40];
    int iterator = 0;
    char* resultfile = strdup(argv[3]);
    //char* resultfile = "result.txt";

    //argv[1] denotes matrixfile
    //argv[2] denotes vectorfile
    //argv[3] denotes resultfile
    //argv[4] denotes K value
    //argv[5] denotes B value which is the size of the buffer

    //initialization of values of each k bufferArray
    for (int i=0; i<k; i++)
    {
        bufferArray[i] = (int *)malloc(bufferSize * sizeof(int));
        currentIndex[i] = 0;
        emptyPlaces[i] = bufferSize;
        occupiedPlaces[i] = 0;
        isAllRead[i] = false;
    }


    FILE* fmatrix = fopen(argv[1],"r");
    //FILE* fmatrix = fopen("m.txt","r");
    int l = calculateLines(fmatrix);// L value: matrix lines

    if(l==0)
    {
      printf("Matrix has size 0, so the result is 0..");
      exit(1);
    }
    printf ("# of lines in the matrix file: %d \n",l);
    resultRows = (int*)malloc(sizeof(int)*l);

    for(int a=0; a<l; a++)
    {
      resultRows[a] = 0;
    }

    int ceilNumber = ceiling(l, k);//s = ceiling(L/K) values from the matrixfile
    printf ("CeilNumber is: %d \n",ceilNumber);

    FILE* vectorPointer = fopen(argv[2],"r");
    //FILE* vectorPointer = fopen("vector.txt","r");

    vectorSize = calculateLines(vectorPointer);

    if (vectorSize ==0) {
	    printf("vector size is 0, so the result is 0...");
	    exit(1);
	}
    printf("vectorsize: %d \n",vectorSize);
    int s = 2*(vectorSize);
    int vectorArray[s];// odd index: row value of vector even index: value of vector
    //vectorArray both holds the row and element value of the vector.
    //I only want to store element value of the vector. Therefore, I created new array
    //int vectorElements[vectorSize];
    vectorElements = (int*)malloc(sizeof(int)*(vectorSize));
    int j = 0;

    //vectorPointer = fopen(argv[2], "r");
    vectorPointer = fopen("vector.txt", "r");
    //main thread puts the vector elements in global array vectorElements
    for (int i = 0; i < s; i++) {
         fscanf(vectorPointer, "%d", &vectorArray[i]);
         if(i%2 == 0)
         {
            continue;
         }
         else
         {
             vectorElements[j] = vectorArray[i];
             j++;

         }
    }
    fclose(vectorPointer);


    for(int i=0; i< vectorSize; i++)//I copied vector elements into array
    {
         printf("Element of vector: %d \n", vectorElements[i]);
    }

    //Next step is to calculate the size of split files
    //And generate split files according to their sizes.
    for(int j=0; j<k; j++)
    {
        sizeArray[j] = ceilNumber;//s = ceiling(L/K) values from the matrixfile
        if(j==(k-1))
        {
          int remaining = l%ceilNumber;
          sizeArray[j] = remaining + ceilNumber; //last split may contain less than S
        }
    }

    //generate split files
    FILE* ptr_readfile = fopen("m.txt","r");
    char buf[15];
    //main thread generates split files
    for(int i=0;i<k;i++){

          sprintf(fileoutputname, "split_file_%d.txt", i);
          splitFiles[i] = fopen(fileoutputname, "w");


          while(iterator<sizeArray[i])
          {
              fgets(buf, 15, ptr_readfile);
              fputs(buf, splitFiles[i]);//split the files according to their sizes.
              iterator++;
          }
          iterator = 0;
          fclose(splitFiles[i]);
    }
    fclose(ptr_readfile);

    //Next step of main thread is to create K mapper threads and 1 reducer thread
    pthread_t mappers[k];//mapper threads are created

    for(int i=0; i<k; i++)
    {
      sem_init(&mapperSem[i], 0, 1);
      sem_init(&empty[i], 0, bufferSize);
      sem_init(&full[i], 0, 0);
    }

    pthread_t reducer;
    (void) pthread_create(&reducer, NULL, processAndMultiply,resultfile);


    for(int i=0;i<k;i++)  {

         sprintf(fileoutputname, "split_file_%d.txt", i);
         struct arg_struct args;
         args.arg1 = fileoutputname;
         args.arg2 = i;
         args.arg3 = sizeArray[i];
         args.arg4 = currentIndex[i];
         args.arg5 = emptyPlaces[i];
         args.arg6 = occupiedPlaces[i];
        (void) pthread_create(&mappers[i], NULL, mapReadPass,(void *)&args);

    }

    //create reducer thread

    for (int i = 0; i < k; i++) {
		(void) pthread_join(mappers[i], NULL);

	}

	  (void) pthread_join(reducer, NULL);

   exit(0);
}

