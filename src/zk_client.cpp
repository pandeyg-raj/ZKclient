#include "../include/zookeeper.h"
#include <errno.h>
#include <string.h>
#include <chrono>
#include <iostream>
#include <thread>
#include <random>
using namespace std::chrono;
using std::cout;
using std::endl;

static int NOOFTHREAD = 10 ;
static int experiment_time = 60;

extern "C"{

ZOOAPI int zoo_get(zhandle_t *zh, const char *path, int watch, char *buffer, int* buffer_len, struct Stat *stat);
ZOOAPI int zoo_set(zhandle_t *zh, const char *path, const char *buffer,int buflen, int version);
ZOOAPI int zoo_create(zhandle_t *zh, const char *path, const char *value, int valuelen, const struct ACL_vector *acl, int mode, char *path_buffer, int path_buffer_len);
ZOOAPI int zoo_exists(zhandle_t *zh, const char *path, int watch, struct Stat *stat);
}
inline int next_event(const std::string& dist_process);
void main_watcher ( zhandle_t *zkh,int type,int state,const char *path,void* context);
void hello(int rc,const char *value,int value_len,const struct Stat *stat, const void *data);
void Stop();
int my_zoo_set(zhandle_t *zh, const char *path, const char *buffer, int buflen, int version,int,FILE*);
int my_zoo_get(zhandle_t *zh, const char *path, int watch, char *buffer,int* buffer_len, struct Stat *stat,int,FILE*);
void thread_function(int );
static int connected = 0;
static int expired = 0;

static int GET_c[64] = {0};
static int SET_c[64] = {0};
static int TOTAL[64] = {0};
zhandle_t * zk = 0;
int main (int argc, char** argv)
{
    experiment_time = atoi(argv[1]);
    NOOFTHREAD = atoi(argv[2]);
    zk =  zookeeper_init("34.94.181.64:2181",main_watcher,15000, 0,0,0);
    if(!zk) {
        printf("\n\nzk null\n\n");
    }
    while(!connected) {
    //printf("client not connected\n");
    }
    if(connected) {
       printf("client  connected\n");
    }
    int count = 0;
    

    std::thread workers[10];
    for(int i =0;i<NOOFTHREAD;i++)
    workers[i] = std::thread(thread_function, i);

    for(int i =0;i<NOOFTHREAD;i++)
    workers[i].join();

    zookeeper_close(zk);
    int totalGet = 0,totalSet = 0 ,total = 0;
    for(int i =0;i<NOOFTHREAD;i++)
    {
        totalGet+= GET_c[i];
        totalSet+= SET_c[i];
        total+= TOTAL[i];
    }

    cout<<"TIME of Experiment "<<experiment_time<<endl<<"THREAD: "<<NOOFTHREAD<<endl;
    cout<<"TOTAL: "<<total<<" GET: "<<totalGet<<" SET: "<<totalSet<<endl;
    cout<<"Read Ratio: "<<((float)totalGet/total)*100<<endl;
    cout<<"rate: "<<(float)total/experiment_time;
    return 0;
}

void thread_function(int i)
{
    TOTAL[i] = 0;
    struct Stat stat;
    int rc = 0;
    char buffer[512] = {0};
    int buflen = sizeof(buffer);
    const char *buffer_set = "rajudatanew";
    int buf_set_len = strlen(buffer_set);

    const char* path = "/raju";
    char filename[64] = {0};
    sprintf(filename, "file%d.txt",i);
    FILE* fptr = fopen(filename,"w");
    auto start_point = time_point_cast<milliseconds>(system_clock::now());
    auto end_point = start_point + seconds(experiment_time);
    time_point <system_clock, milliseconds> tp = time_point_cast<milliseconds>(system_clock::now());
    int type;
    while(system_clock::now() < end_point ){
       TOTAL[i]++;
       type = TOTAL[i]%2;
       if(type==0){
        my_zoo_get(zk,path,0,buffer,&buflen,&stat,i,fptr); // check for exist internally
        GET_c[i]++;
       }
       else{
         my_zoo_set(zk,path, buffer_set,buf_set_len, -1,i,fptr);
         SET_c[i]++;
       }
        tp += milliseconds{next_event("poisson")};
        std::this_thread::sleep_until(tp);
    }
    fprintf(fptr,"Thread#: %d GET#: %d  SET#: %d    TOTAL#: %d\n",i,GET_c[i],SET_c[i],TOTAL[i]);
    fclose(fptr);
}
int my_zoo_set(zhandle_t *zk, const char *path, const char *buffer,
                   int buflen, int version,int i, FILE* fptr)
{
    struct Stat stat;
    int rc = 0;
    auto m_Start = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();

    if ( zoo_exists(zk, path, 0, &stat) == 0) { // node exist setting value
     rc =  zoo_set(zk,path,buffer,buflen,version); // check for exist internally
        if(rc!=0) 
            fprintf(fptr,"my_zoo_set error   %d \n",rc);
    }
    else {
        fprintf (fptr,"node does not exist, creating and setting \n ");
        rc= zoo_create(zk,path,buffer, buflen, &ZOO_OPEN_ACL_UNSAFE, ZOO_PERSISTENT, NULL, 0);
        if(rc!=0) 
            fprintf(fptr,"my_zoo_create error   %d \n",rc);
    }
    auto m_End = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    fprintf(fptr,"SET: No error thread# %d,   start: %ld,   end: %ld,   diff: %ld, data%s\n",i,m_Start,m_End,m_End-m_Start,buffer);
        
    return rc;
}





int my_zoo_get(zhandle_t *zk, const char *path, int watch, char *buffer,
                   int* buflen, struct Stat *stat,int i,FILE* fptr)
{
     int rc =0;
        auto m_Start = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
             rc =  zoo_get(zk,path,0,buffer,buflen,stat); // check for exist internally
        auto m_End = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
        if(rc==0) 
        {
            fprintf(fptr,"GET: No error thread# %d,   start: %ld,   end: %ld,   diff: %ld, data%s\n",i,m_Start,m_End,m_End-m_Start,buffer);
        }
        else 
            fprintf(fptr,"my_zoo_get error   %d \n",rc);
        return rc;
}

void main_watcher ( zhandle_t *zkh,
                    int type,
                    int state,
                    const char *path,
                    void* context)
{
    printf("\n\nwatcher called\n\n");
    if (type == ZOO_SESSION_EVENT) 
    {
        if (state == ZOO_CONNECTED_STATE) 
        {
            connected = 1; 
        } 
        else if (state == ZOO_NOTCONNECTED_STATE ) 
        {
            connected = 0;
        } 
        else if (state == ZOO_EXPIRED_SESSION_STATE) 
        {
            expired = 1; 
            connected = 0;
            zookeeper_close(zkh);
        }    
    }
}
 void hello(int rc, 
        const char *value, 
        int value_len,
        const struct Stat *stat, 
        const void *data)
        {
            printf("hello");
        }


inline int next_event(const std::string& dist_process){
    if(dist_process == "poisson"){
        std::default_random_engine generator(system_clock::now().time_since_epoch().count());
#ifdef DEBUGGING
        std::exponential_distribution<double> distribution(1);
#else
        std::exponential_distribution<double> distribution(.5);
#endif

        return duration_cast<milliseconds>(duration<double>(distribution(generator))).count();
//        return -log(1 - (double)rand() / (RAND_MAX)) * 1000;
    }
    else{
        throw std::logic_error("Distribution process specified is unknown !! ");
    }
    return 0;
}