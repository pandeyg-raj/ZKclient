#include <chrono>
#include <iostream>
class Timer
{
public:
    Timer()
    {
        m_Start = std::chrono::high_resolution_clock::now();
    }
  
    ~Timer()
    {
        Stop();
    }
    void Stop()
    {
        auto endTime = std::chrono::high_resolution_clock::now();
        auto start = std::chrono::time_point_cast<std::chrono::microseconds>(m_Start).time_since_epoch().count();
        auto end = std::chrono::time_point_cast<std::chrono::microseconds>(endTime).time_since_epoch().count();
        auto duration = end - start;
        double ms = duration * 0.001;

        std::cout << duration << "us (" <<ms<<" ms)\n";
    }
private:
    std::chrono::time_point<std::chrono::high_resolution_clock> m_Start;
};