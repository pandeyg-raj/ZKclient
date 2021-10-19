#include "../include/zookeeper.h"
#include <errno.h>
ZOOAPI int zoo_get(zhandle_t *zh, const char *path, int watch, char *buffer,
                   int* buffer_len, struct Stat *stat);
static int connected = 0;
static int expired = 0;

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
 void hell(int rc, 
        const char *value, 
        int value_len,
        const struct Stat *stat, 
        const void *data)
        {
            printf("hell");
        }
int main (void)
{
    zhandle_t * zk = 0;
    zk =  zookeeper_init("localhost:2181", 
    main_watcher, 
    15000, 
    0, 
    0, 
    0);

    if(!zk)
    {
    printf("\n\nzk null\n\n");
    }
    
   while(!connected)
   {
    //printf("client not connected\n");
   }
   if(connected)
   {
       printf("client  connected\n");
   }
    int rc = 0;
    char buffer[512];
    int buflen = sizeof(buffer);
    struct Stat stat;
    const char* path = "/raj";
    //int rc = zoo_aget(zk,"raj",0,hell,buffer);
    if ( zoo_exists(zk, path, 0, &stat) == 0)
    {
        printf ("node exist, following is data \n ");
        if( zoo_get(zk,path,0,buffer,&buflen,&stat)==0)
            puts(buffer);
        else
        {
            printf("error %d \n",rc);
        }
    }
    else
    {
        printf ("node does not exist \n ");
    }
    
    zookeeper_close(zk);
    return 0;
}