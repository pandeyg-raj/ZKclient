#include "../include/zookeeper.h"
#include "../include/g2logworker.h"
#include "../include/g2log.h"
#include <iomanip>
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
static double durations[256] = {0.0};
static double getdurations[256] = {0.0};
static double setdurations[256] = {0.0};
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
int my_zoo_set(zhandle_t *zh, const char *path, const char *buffer, int buflen, int version,int);
int my_zoo_get(zhandle_t *zh, const char *path, int watch, char *buffer,int* buffer_len, struct Stat *stat,int);
void thread_function(int );
static int connected = 0;
static int expired = 0;

static int GET_c[256] = {0};
static int SET_c[256] = {0};
static int TOTAL[256] = {0};

zhandle_t * zk = 0;
int main (int argc, char** argv)
{
    if(argc!=3){
        cout<<"usage: ./a.out {time} {#thread}"<<endl;
        return -1;
    }
    g2LogWorker g2log(argv[0], "./");
    g2::initializeLogging(&g2log);
    
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
    
    LOG(INFO) <<"            thread,start,end,diff,data";

    std::thread workers[NOOFTHREAD];
    for(int i =0;i<NOOFTHREAD;i++)
    workers[i] = std::thread(thread_function, i);

    for(int i =0;i<NOOFTHREAD;i++)
    workers[i].join();

    zookeeper_close(zk);
    int totalGet = 0,totalSet = 0 ,total = 0;
    double totalLatency = 0.0,getLatency = 0.0,setLatency = 0.0;
    for(int i =0;i<NOOFTHREAD;i++)
    {
        durations[i] = getdurations[i] +  setdurations[i];

        totalGet += GET_c[i];
        totalSet += SET_c[i];

        total+= TOTAL[i];
        
        getLatency += getdurations[i];
        setLatency += setdurations[i];
        totalLatency += durations[i];
    }

    LOG(INFO)<<"    TIME of Experiment "<<experiment_time;
    LOG(INFO)<<"    THREAD: "<<NOOFTHREAD;
    LOG(INFO)<<"    TOTAL: "<<total<<" GET: "<<totalGet<<" SET: "<<totalSet;
    LOG(INFO)<<"    Read Ratio: "<<((float)totalGet/total)*100;
    LOG(INFO)<<"    rate: "<<(float)total/experiment_time;
    LOG(INFO)<<"    avg latency (total)(ms) "<< (totalLatency*0.001)/total;
    LOG(INFO)<<"    avg GET latency (ms) "<< (getLatency*0.001)/totalGet;
    LOG(INFO)<<"    avg SET latency (ms) "<< (setLatency*0.001)/totalSet;
    

    cout<<"TIME of Experiment "<<experiment_time<<endl<<"THREAD: "<<NOOFTHREAD<<endl;
    cout<<"TOTAL: "<<total<<" GET: "<<totalGet<<" SET: "<<totalSet<<endl;
    cout<<"Read Ratio: "<<((float)totalGet/total)*100<<endl;
    cout<<"rate: "<<(float)total/experiment_time<<endl;
    cout<<"avg latency (total)(ms) "<< (totalLatency*0.001)/total<<endl;
    cout<<"avg GET latency (ms) "<< (getLatency*0.001)/totalGet<<endl;
    cout<<"avg SET latency (ms) "<< (setLatency*0.001)/totalSet<<endl;
    return 0;
}

void thread_function(int i)
{
    TOTAL[i] = 0;
    GET_c[i] = 0;
    SET_c[i] = 0;
    struct Stat stat;
    int rc = 0;
    char buffer[1024] = {0};
    int buflen = sizeof(buffer);
    const char *buffer_set = "What about this random String ? You are sure, right? But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born and I will give you a complete account of the system, and expound the actual teachings of the great explorer of the truth, the master-builder of human happiness. No one rejects, dislikes, or avoids pleasure itself, because it is pleasure, but because those who do not know how to pursue pleasure rationally encounter consequences that are extremely painful. Nor again is there anyone who loves or pursues or desires to obtain pain of itself, because it is pain, but because occasionally circumstances occur in which toil and pain can procure him some great pleasure. To take a trivial example, which of us ever undertakes laborious physical exercise, except to obtain some advantage from it? But who has any right to find fault with a man who chooses to enjoy a pleasure that has no annoying consequences, or one who avoids a pain that produces no resultant pleasure?";
    int buf_set_len = strlen(buffer_set);

    const char* path = "/raju";
    char filename[64] = {0};
    sprintf(filename, "./results/file%d.txt",i);
    //FILE* fptr = fopen(filename,"w");
    //fprintf(fptr,"thread,start,end,diff,data\n");
    
    auto start_point = time_point_cast<milliseconds>(system_clock::now());
    auto end_point = start_point + seconds(experiment_time);
    time_point <system_clock, milliseconds> tp = time_point_cast<milliseconds>(system_clock::now());
    int type;
    while(system_clock::now() < end_point ){
       TOTAL[i]++;
       type = TOTAL[i]%2;
       if(type==0){
        my_zoo_get(zk,path,0,buffer,&buflen,&stat,i); // check for exist internally
        GET_c[i]++;
       }
       else{
         my_zoo_set(zk,path, buffer_set,buf_set_len, -1,i);
         SET_c[i]++;
       }
        tp += milliseconds{next_event("poisson")};
        std::this_thread::sleep_until(tp);
    }
    //fprintf(fptr,"Thread#: %d GET#: %d  SET#: %d    TOTAL#: %d\n",i,GET_c[i],SET_c[i],TOTAL[i]);
    //LOG(INFO) << "            Thread#: "<<i<<" GET#: "<<GET_c[i]<<"  SET#: "<<SET_c[i]<<"    TOTAL#: "<<TOTAL[i];
    
    //fclose(fptr);
}
int my_zoo_set(zhandle_t *zk, const char *path, const char *buffer,
                   int buflen, int version,int i)
{
    struct Stat stat;
    int rc = 0;
    auto m_Start = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();

    if ( zoo_exists(zk, path, 0, &stat) == 0) { // node exist setting value
     rc =  zoo_set(zk,path,buffer,buflen,version); // check for exist internally
        if(rc!=0) 
            printf("my_zoo_set error   %d \n",rc);
    }
    else {
        printf ("node does not exist, creating and setting \n ");
        rc= zoo_create(zk,path,buffer, buflen, &ZOO_OPEN_ACL_UNSAFE, ZOO_PERSISTENT, NULL, 0);
        if(rc!=0) 
            printf("my_zoo_create error   %d \n",rc);
    }
    auto m_End = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    setdurations[i]+= m_End-m_Start;
    //fprintf(fptr,"SET: No error thread# %d,   start: %ld,   end: %ld,   diff: %ld, data%s\n",i,m_Start,m_End,m_End-m_Start,buffer);
      //LOG(INFO) <<" SET#"<<i<<","<<m_Start<<","<<m_End<<","<<m_End-m_Start<<","<<buffer;  
      LOG(INFO) <<"            SET#"<<i<<","<<m_Start<<","<<m_End<<","<<m_End-m_Start<<","<<buffer;  
      
    return rc;
}





int my_zoo_get(zhandle_t *zk, const char *path, int watch, char *buffer,
                   int* buflen, struct Stat *stat,int i)
{
     int rc =0;
        auto m_Start = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
             rc =  zoo_get(zk,path,0,buffer,buflen,stat); // check for exist internally
        auto m_End = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
        if(rc==0) 
        {
            //fprintf(fptr,"GET: No error thread# %d,   start: %ld,   end: %ld,   diff: %ld, data%s\n",i,m_Start,m_End,m_End-m_Start,buffer);
            getdurations[i]+= m_End-m_Start;
            //fprintf(fptr,"GET#%d,%ld,%ld,%ld,%s\n",i,m_Start,m_End,m_End-m_Start,buffer); 
            LOG(INFO) <<"            GET#"<<i<<","<<m_Start<<","<<m_End<<","<<m_End-m_Start<<","<<buffer;
          
        }
        else 
            printf("my_zoo_get error   %d \n",rc);
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
