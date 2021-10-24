#include "../include/g2logworker.h"
#include "../include/g2log.h"
#include <iomanip>
#include <thread>

int main(int argc, char** argv)
{
    g2LogWorker g2log(argv[0], "./");
    g2::initializeLogging(&g2log);
    LOG(INFO) << "Simple to use with streaming syntax, easy as ABC or " ;
}