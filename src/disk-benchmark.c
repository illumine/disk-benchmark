/***********************************************************************
Filename : disk-benchmark.c           
Description :
    A tool to test and benchmark storage (SAN, SCSI, NAS, Local, SSDs) 
Build :
 # gcc disk-benchmark.c -o disk-benchmark  -l pthread -O3  -Wall
 
Author: 
    Michael Mountrakis mike.mountrakis@gmail.com
**************************************************************************/
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h> 
#include <ctype.h> 
#include <sys/types.h> 
#include <sys/stat.h> 
#include <fcntl.h> 
#include <time.h> 
#include <pthread.h> 
#include <unistd.h> 

#define THREADS_DEF 5 
#define FILE_SIZ_MIN_DEF 1024 
#define FILE_SIZ_MAX_DEF 10240 
#define REPEATS_DEF 5 
#define SLEEP_SEC_DEF 1
#define WORK_CONTINOUSLY_SLEEP_BRAKE_DEF 5

#define ER_EXIT 1

#define VERSION "v2.0"
#define BUILD_DATE "7/2/2021"

/* We define this as a macro since we need to have it inline for making calculation faster from a function*/
#define TDIFF(tstart,tend)  (((tend.tv_sec) + (1.0e-9)*(tend.tv_nsec)) - ((tstart.tv_sec) + (1.0e-9)*(tstart.tv_nsec)))



typedef struct ThreadWork_t{
 /* Thread ID */
    int tid;
 /* Input from the Command line - User Options */
    char * path;
    int threads;
    size_t file_min, file_max;
    size_t file_absolute;
    size_t repeats;
    size_t buffer_siz;
    size_t buffer_siz_w;
    size_t buffer_siz_r;
    int    write_only;
    int    delete_files;
    unsigned int sleep_seconds;
    unsigned int sleep_sec_min, sleep_sec_max;
    int    values_only;
    int    dont_print_scenario_info;
    int    dont_print_clocks;
    int    dont_print_headers;
    int    print_date;
    int    work_continously;
    unsigned int work_continously_sleep_brake;
 
    /* Results */
 /* Average time in sec to write the file */
    long double avg_file_w;  
 /* Average time in sec to read from a file */
    long double avg_file_r;  
 /* Total thread time in sec to write the file <repeat> times */
    long double total_file_w; 
 /* Total thread time in sec to read the file <repeat> times */
    long double total_file_r;
 /* Average time in sec the thread was sleep <repeat> times */
    long double sleep_for_sec;
 /* Average file size in bytes the thread wrote <repeat> times */
    long double avg_file_siz;
}
ThreadWork_t;

void print_version( void ){
        printf("\nVersion %s  built on %s\n",VERSION,BUILD_DATE);
}


void usage( char * progname){
    printf("\n%s -p <path> -t <threads number> -r <repeats per thread>  -b <buffer size> -l <file size lower> -u <file size upper> -a <absolute file size> ",progname);
    printf("\n Makes a disk benchmark on <path> by creating <threads number> threads (default is 5 threads))");
    printf("\n Each thread first writes and then reads a file of size between <file size lower> and <file size upper> OR  <absolute file size>");
    printf("\n for <repeats per thread> times. Thread is reading/ writing the file using I/O buffer of <buffer size> bytes");
    printf("\n The resulting Time for Reading/Writing the file for each thread and summary results are printed.");
    printf("\nResults Headers");
    printf("\nThread\tAvg W\tAvg R\tTotal W\tTotal R\tTotal Time\tSleep\tAvg File Size");
    printf("\n Other options");
    printf("\n -e <sleep seconds> Make each thread sleep for <sleep seconds> before attempt to read.");
    printf("\n -E <sleep sec min>:<sleep sec max> Make each thread sleep for randomly picked sec between <sleep sec min>:<sleep sec max> before attempt to read.");
    printf("\n -w Make each thread do write only.");
    printf("\n -n Make each thread NOT delete the sample files. MAKE SURE YOU HAVE ENOUGH SPACE if you turn this option!");
    printf("\n -B <write buff size>:<read buffer size> Make each thread write/read files with those buffer sizes."); 
    printf("\n -x print values only. Use this to grab values for spreadsheets.");  
    printf("\n -o Don't print clocks.");
    printf("\n -j Don't print scenario information.");
    printf("\n -h Don't print headers.");
    printf("\n -d print system local time also.");
    printf("\n -c work continuously. Blocks in a loop recreating threads without printing summaries. You have to interrupt with Cntrl-C");
    printf("\n -z <sleep seconds> Sleep interval time in seconds when work continuously. Default is 5 seconds. Setting this to somthing small, will default to high CPU load."); 
    printf("\n%s -s ",progname);
    printf("\n Prints information around system I/O. Use this info to set R/W buffer size for your tests with -b option."); 
    printf("\n Writing to a file in smaller chunks may cause an inefficient read-modify-rewrite.");
    printf("\n Returns 0 on OK , 1 on Error.");
    printf("\n Author: Michael Mountrakis 2021 - mike.mountrakis@gmail.com");
    print_version();
    printf("\n");
}


void init_thread_work( ThreadWork_t *thread_work){
    thread_work->tid = -1; /* denotes main */
    thread_work->path = NULL;
    thread_work->threads = THREADS_DEF;
    thread_work->file_min = FILE_SIZ_MIN_DEF;
    thread_work->file_max = FILE_SIZ_MAX_DEF;
    thread_work->file_absolute = 0;
    thread_work->repeats = REPEATS_DEF;
    thread_work->buffer_siz = BUFSIZ;
    thread_work->buffer_siz_w = 0;
    thread_work->buffer_siz_r = 0;
    thread_work->avg_file_w = 0;
    thread_work->avg_file_r = 0;
    thread_work->total_file_w = 0;
    thread_work->total_file_r = 0;
    thread_work->write_only = 0;
    thread_work->sleep_seconds =  SLEEP_SEC_DEF;
    thread_work->delete_files = 1;
    thread_work->values_only = 0;
    thread_work->dont_print_scenario_info = 0;
    thread_work->dont_print_clocks = 0;
    thread_work->dont_print_headers = 0;
    thread_work->print_date = 1;
    thread_work->work_continously = 0;
    thread_work->work_continously_sleep_brake = WORK_CONTINOUSLY_SLEEP_BRAKE_DEF;
	
    thread_work->sleep_sec_min = 0;
    thread_work->sleep_sec_max = 0;
    thread_work->sleep_for_sec = 0;
    thread_work->avg_file_siz = 0;
}

void copy_thread_work(ThreadWork_t * from, ThreadWork_t *to, int id ){
    to->tid = id;
    to->path = from->path;
    to->threads = from->threads;
    to->file_min = from->file_min;
    to->file_max = from->file_max;
    to->file_absolute = from->file_absolute;
    to->repeats = from->repeats;
    to->buffer_siz = from->buffer_siz;
    to->buffer_siz_w = from->buffer_siz_w;
    to->buffer_siz_r = from->buffer_siz_r;
    to->write_only = from->write_only;
    to->sleep_seconds = from->sleep_seconds;
    to->delete_files = from->delete_files;
    to->values_only = from->values_only;
    to->dont_print_scenario_info = from->dont_print_scenario_info;
    to->dont_print_clocks = from->dont_print_clocks;
    to->dont_print_headers = from-> dont_print_headers;
    to->print_date = to->print_date;
    to->work_continously = from->work_continously;
    to->work_continously_sleep_brake = from->work_continously_sleep_brake;
 
    to->sleep_sec_min = from->sleep_sec_min;
    to->sleep_sec_max = from->sleep_sec_max;
    to->sleep_for_sec = from->sleep_for_sec;
    to->avg_file_siz = from->avg_file_siz;
}

/* Show what has been done, results */
void debug_thread_results(ThreadWork_t * thread_work){
    if( !thread_work->values_only )
#ifdef __linux__    
        printf("\nT=%d, Avg W=%Lf Avg R=%Lf Total W=%Lf Total R=%Lf Total Time=%Lf Sleep=%Lf  Avg File Size =%Lf",
            thread_work->tid,
            thread_work->avg_file_w,
            thread_work->avg_file_r,
            thread_work->total_file_w,
            thread_work->total_file_r,
            thread_work->total_file_w + thread_work->total_file_r,
            thread_work->sleep_for_sec,
            thread_work->avg_file_siz);         
#else    
/*  Windows long double issue. See https://stackoverflow.com/questions/19952200/scanf-printf-double-variable-c  */
        printf("\nT=%d, Avg W=%f Avg R=%f Total W=%f Total R=%f Total Time=%f Sleep=%f  Avg File Size =%f",
            thread_work->tid,
           (double) thread_work->avg_file_w,
           (double) thread_work->avg_file_r,
           (double) thread_work->total_file_w,
           (double) thread_work->total_file_r,
           (double) (thread_work->total_file_w + thread_work->total_file_r),
           (double) thread_work->sleep_for_sec,
           (double) thread_work->avg_file_siz);
#endif           
    else
         printf("\n%d\t%Lf\t%Lf\t%Lf\t%Lf\t%Lf\t%Lf\t%Lf",
            thread_work->tid,
            thread_work->avg_file_w,
            thread_work->avg_file_r,
            thread_work->total_file_w,
            thread_work->total_file_r,
            thread_work->total_file_w + thread_work->total_file_r,
            thread_work->sleep_for_sec,
            thread_work->avg_file_siz); 
}

void debug_thread_results_header(ThreadWork_t * thread_work, int is_summaries) {
    if( is_summaries && !thread_work->dont_print_headers && thread_work->values_only)
        printf("\nSummaries - Averages all threads");
    else
        printf("\n");
  
    if( !thread_work->dont_print_headers && thread_work->values_only  ) 
        printf("\nThread\tAvg W\tAvg R\tTotal W\tTotal R\tTotal Time\tSleep\tAvg File Size");
}

/* This one prints only the input parameters */
void debug_thread_work(ThreadWork_t * thread_work){
 if( !thread_work->dont_print_scenario_info )
        printf( "\nTest scenario:\ntest path=%s \
                 \nThreads=%d, sleep sec between write/read = %u, repeats per thread=%ld, random pick sleep sec from [%u %u]\
                 \nLower file size=%ld, Upper file size=%ld, Absolute file size=%ld \
                 \nRead/Write buffer size=%ld,  Buff Siz W %ld, Buf Siz R %ld,\
                 \nDo write only=%d, Delete files=%d \
                 \nPrint values only=%d dont print scenario info= %d, dont print clocks=%d dont print headers=%d print date=%d\
                 \nWork Continously=%d  Work Continously Sleep Brake=%u",
        thread_work->path,
        thread_work->threads,
        thread_work->sleep_seconds, 
        thread_work->repeats,
        thread_work->sleep_sec_min,
        thread_work->sleep_sec_max,
        thread_work->file_min,
        thread_work->file_max,
        thread_work->file_absolute,
        thread_work->buffer_siz,
        thread_work->buffer_siz_w,
        thread_work->buffer_siz_r,
        thread_work->write_only,
        thread_work->delete_files,
        thread_work->values_only,
        thread_work->dont_print_scenario_info,
        thread_work->dont_print_clocks,
        thread_work->dont_print_headers,
        thread_work->print_date,
        thread_work->work_continously,
        thread_work->work_continously_sleep_brake
  );
}

/* Prints system date*/
void print_system_datetime(void ){
  time_t rawtime;
  struct tm * timeinfo;

    time ( &rawtime );
    timeinfo = localtime ( &rawtime );
    printf ( "\n%s", asctime (timeinfo) );
}

/*  Prints system I/O info */ 
void print_system_io_info( char * filepath ){
  struct stat fileStat;
  long blocksize;
  
    if(stat(filepath,&fileStat) < 0){
        perror("\nCould not stat!");
        return;
    }
    
#ifdef __linux__    
    blocksize = fileStat.st_blksize;
#else
   blocksize = BUFSIZ;
#endif 
    /* See http://www.tutorialspoint.com/unix_system_calls/stat.htm */
    printf("\nSystem I/O Block Site (stat.h : struct stat st_blksize) is %ld bytes",blocksize);
    /* See http://www.cplusplus.com/reference/cstdio/BUFSIZ/ */
    printf("\nSystem Buffer Size (stdio.h : BUFSIZ) is %d bytes",BUFSIZ); 
    printf("\n");
}


int parse_cli_arguments( ThreadWork_t *thread_work, int argc, char *argv[]){
  int i, opt;
  opterr = 0;
  
    if( argc == 1 ){
     usage(argv[0]);
     exit(0);
    }
  
    while ((opt = getopt (argc, argv, "p:t:a:l:u:r:b:B:e:E:z:snwvxiojhdc")) != -1){
        switch (opt) {
            case 'p':
                thread_work->path = strdup(optarg);
                break;
            case 't': 
                thread_work->threads = atoi(optarg);
                break;   
            case 'l': 
                thread_work->file_min = atoi(optarg);
                break;
            case 'u': 
                thread_work->file_max = atoi(optarg);
                break;
            case 'r': 
                thread_work->repeats = atoi(optarg);
                break;
            case 'b': 
                thread_work->buffer_siz = atoi(optarg);
                break;
            case 'B':
                thread_work->buffer_siz = 0;
                sscanf(optarg,"%ld:%ld",&thread_work->buffer_siz_w, &thread_work->buffer_siz_r);
                break;                
            case 'a': 
                thread_work->file_absolute = atoi(optarg);
                break;
            case 's': 
                print_system_io_info( argv[0]);
                exit(0);
            case 'v': 
                print_version();
                exit(0);                
            case 'w':
                thread_work->write_only = 1;
                break;
            case 'n':
                thread_work->delete_files = 0;
                break;
            case 'x':
                thread_work->values_only = 1;
                break;                
            case 'e':
                thread_work->sleep_seconds = atoi(optarg);
                break;
            case 'o':
                thread_work->dont_print_clocks=1;
                break;  
            case 'j':
                thread_work->dont_print_scenario_info=1;
                break; 
            case 'h':
                thread_work->dont_print_headers=1;
                break;  
            case 'd':
                thread_work->print_date=1;
                break;    
            case 'E':
                sscanf(optarg,"%d:%d",&thread_work->sleep_sec_min, &thread_work->sleep_sec_max);
                break;
            case 'c':
                thread_work->work_continously=1;
                break;
            case 'z':
                thread_work->work_continously_sleep_brake = atoi(optarg);
                break;
            default: 
               usage(argv[0]);
               abort ();
        }
    }
    debug_thread_work(thread_work);    

    for (i = optind;i < argc; i++)  
      printf ("\nNon-option argument %sn", argv[i]);
    
    return 0;
}

/* Beware rand() and others are *NOT* thread safe. Do that trick with the seed 
   so that each thread has its own generator */
size_t random_between_range( size_t min, size_t max ){
#ifdef __linux__
  unsigned short state[3];
  unsigned int seed = time(NULL) + (unsigned int) pthread_self();
     memcpy(state, &seed, sizeof(seed));
     return min +  nrand48(state) % (max - min );
#else
  time_t t;
    srand((unsigned) time(&t) + (unsigned int) pthread_self() );
    return min +  rand() % (max - min );
#endif
}


void write_file( const char * path, size_t file_size, size_t buffer_siz ){
  int fd = -1;
  char msg[BUFSIZ] = {'\0'};
  int i, times = file_size / buffer_siz + 1 ;
  char * data = (char *)calloc( buffer_siz, sizeof(char) );
  size_t bytes = 0;
  
    fd = open(path, O_WRONLY | O_CREAT , 0644);
    if( fd < 0 ){
        sprintf(msg,"\nError opening file %s for writing.", path);
        perror(msg);
        return;
    }else {
        ;
        /* printf("nfile %s opened for writing.",path);  */
    }
 	
    for (i = 0;i < times; i++){
        bytes += write(fd, data, buffer_siz);
    }
    close(fd);
    free(data);
}

void read_file( const char * path, size_t buffer_siz ){
  int fd;
  int times_read = 0;
  char msg[BUFSIZ] = {'\0'};
  char * data = (char *)calloc( buffer_siz, sizeof(char) );
  
    fd = open(path, O_RDONLY, 0);  
    if( fd < 0 ){
        sprintf(msg,"\nError opening file %s for reading.", path);
        perror(msg);
        return;
    }
    else{
        ;
        /* printf("nfile %s opened for reading.",path);  */
    }
    while (read(fd, data, buffer_siz) > 0 ){
        times_read++;
    }
    close(fd);
    free(data);
}

void * do_benchmark(void *tw){
  ThreadWork_t * work = (ThreadWork_t *) tw;
  char filepath[BUFSIZ] = {'\0'};
  size_t file_size = 0;
  int i;
  unsigned int sleep_sec=0;
  struct timespec tstart, tend;
  
  /* 
    printf("\nThread #%d!", work->tid);
    debug_thread_work(work);
  */
   
   for(i=0; i<work->repeats; i++ ){
#ifdef  __linux__   
      sprintf(filepath,"%s/thread%d-%d",work->path,work->tid,i);
#else
      sprintf(filepath,"%s\\thread%d-%d",work->path,work->tid,i);
#endif     
        if( work->file_absolute ) 
            file_size = work->file_absolute;
        else 
            file_size = random_between_range( work->file_min, work->file_max);
        work->avg_file_siz += file_size;
   
        /* write  the file*/    
        clock_gettime(CLOCK_MONOTONIC, &tstart);
        if( !work->buffer_siz)
            write_file( filepath, file_size, work->buffer_siz_w );
        else
            write_file( filepath, file_size, work->buffer_siz );
        write_file( filepath, file_size, work->buffer_siz );
        clock_gettime(CLOCK_MONOTONIC, &tend);
        work->total_file_w += TDIFF( tstart, tend);
        
  /* decide how long to sleep before reading the file */
        if( work->sleep_seconds )
            sleep_sec = work->sleep_seconds;
        if ( work->sleep_sec_min && work->sleep_sec_max)
            sleep_sec = random_between_range(work->sleep_sec_min, work->sleep_sec_max);  
        work->sleep_for_sec += sleep_sec;
        if( sleep_sec )
            sleep(sleep_sec);  
        
        /* read the file */
        if( !work->write_only ){
            clock_gettime(CLOCK_MONOTONIC, &tstart);
            if( !work->buffer_siz)
                read_file( filepath, work->buffer_siz_r );
            else
                read_file( filepath, work->buffer_siz );
            clock_gettime(CLOCK_MONOTONIC, &tend);
            work->total_file_r += TDIFF( tstart, tend);
        }
     
     /* remove the file */
        if( work->delete_files )
            remove(filepath);
   }
  
    /* calculate averages */
    work->avg_file_w = work->total_file_w / work->repeats;
    work->avg_file_r = work->total_file_r / work->repeats;
    work->sleep_for_sec = work->sleep_for_sec / work->repeats;
    work->avg_file_siz  = work->avg_file_siz / work->repeats;
    
 /* print thread results */
    debug_thread_results(work);
    
    pthread_exit(NULL);
}


 /* Calculate summaries from all threads */ 
ThreadWork_t calculate_summaries( ThreadWork_t * results, ThreadWork_t  *threads_work, int thread_num ){
  int t;
  
    for(t=0; t<thread_num; t++) {
        results->avg_file_r    += threads_work[t].avg_file_r;
        results->avg_file_w    += threads_work[t].avg_file_w;
        results->total_file_r  += threads_work[t].total_file_r;
        results->total_file_w  += threads_work[t].total_file_w;
        results->sleep_for_sec += threads_work[t].sleep_for_sec;
        results->avg_file_siz  += threads_work[t].avg_file_siz;
    }
    results->avg_file_r    = results->avg_file_r / thread_num;
    results->avg_file_w    = results->avg_file_w / thread_num;
    results->total_file_r  = results->total_file_r / thread_num;
    results->total_file_w  = results->total_file_w / thread_num;
    results->sleep_for_sec = results->sleep_for_sec / thread_num;
    results->avg_file_siz  = results->avg_file_siz / thread_num;
    
    return *results;
}



 
int main(int argc, char *argv[]){
  pthread_t *threads = NULL;
  ThreadWork_t thread_work, *threads_work = NULL;
  int rc, t;
  time_t start_time, stop_time;
  clock_t start_clock, stop_clock;

    /* Initialize the work and parse user options */
    init_thread_work(&thread_work);
    if( parse_cli_arguments( &thread_work, argc, argv) ){
        fprintf(stderr,"\nError while parsing arguments!\n");
        exit(ER_EXIT);
    }
 
    /* if work continously, just block in the loop waiting for user to interrupt with Cntr-C */
    if( thread_work.work_continously){
        while(1){
            threads =  (pthread_t*)calloc( thread_work.threads, sizeof(pthread_t) );
            threads_work =  (ThreadWork_t*)calloc( thread_work.threads, sizeof(ThreadWork_t) );
            for(t=0; t<thread_work.threads; t++) {
                copy_thread_work(&thread_work, &threads_work[t], t );
                rc = pthread_create( &threads[t], NULL, do_benchmark, (void *) &threads_work[t] );
                if (rc){
                    fprintf(stderr,"\nError return code %d from pthread_create()",rc);
                    exit(ER_EXIT);
                }
            }  
            for(t=0; t<thread_work.threads; t++) 
                pthread_join( threads[t], NULL); 
            free(threads_work);
            free(threads);
            printf("\n");   
            sleep(thread_work.work_continously_sleep_brake);
        }
    }
    
    /* Start Wall and CPU clocks and fire threads */
    start_time = time(NULL);
    start_clock = clock();
    threads = (pthread_t*)calloc( thread_work.threads, sizeof(pthread_t) );
    threads_work = (ThreadWork_t *)calloc( thread_work.threads, sizeof(ThreadWork_t) );
    for(t=0; t<thread_work.threads; t++) {
        copy_thread_work(&thread_work, &threads_work[t], t );
        rc = pthread_create( &threads[t], NULL, do_benchmark, (void *) &threads_work[t] );
        if (rc){
            fprintf(stderr,"\nERROR return code %d from pthread_create()",rc);
            exit(ER_EXIT);
        }
    }


    /* Print headers for threads */
    debug_thread_results_header(threads_work,0);

    /* wait for all threads to finish their job and print their results */
    for(t=0; t<thread_work.threads; t++) 
        pthread_join( threads[t], NULL);

    /* Calculate thread result summaries */
    thread_work = calculate_summaries( &thread_work, threads_work, thread_work.threads );
     
     /* Print headers for summaries and results */
    debug_thread_results_header(threads_work,1);    
    debug_thread_results( &thread_work );

    /* Stop wall and CPU clocks and print times */
    stop_time = time(NULL);
    stop_clock = clock();
    if(!thread_work.dont_print_clocks)
        printf("\nWall time %lf, CPU time %f",  difftime(stop_time, start_time), (double)(stop_clock - start_clock )/CLOCKS_PER_SEC ); 

    if( thread_work.print_date)
        print_system_datetime();
    /* Dealocate resources */
    free(threads_work);
    free(threads);

    /* Last thing that main() should do */ 
    pthread_exit(NULL);
    return 0;
}


/* test routine to check the do_benchmark() routine */
int main_single(int argc, char *argv[]){
  ThreadWork_t thread_work;
    
    init_thread_work(&thread_work);
    if( parse_cli_arguments( &thread_work, argc, argv) ){
     fprintf(stderr,"\nError while parsing arguments!\n");
        exit(ER_EXIT);
    }
    
    thread_work.tid = 0;
    do_benchmark( (void *) &thread_work );
    printf("\nFinished.n");
    return 0;
}


/* Test drive routine for TDIFF macro*/

void test_tdiff( void ){
 struct timespec tstart={0,0}, tend={0,0};
 
    clock_gettime(CLOCK_MONOTONIC, &tstart);
    sleep(5);
    clock_gettime(CLOCK_MONOTONIC, &tend);
    printf("Sleep for ------>  %.5f seconds\n", TDIFF( tstart, tend) );
}

/* Argh! */