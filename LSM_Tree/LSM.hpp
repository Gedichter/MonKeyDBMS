//
//  LSM.hpp
//  LSM_Tree
//
//  Created by Shiyu Huang on 2/21/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#ifndef LSM_hpp
#define LSM_hpp

#include <stdio.h>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>

namespace parameters
{
    const unsigned int BUFFER_CAPACITY = 3;
    const unsigned int SIZE_RATIO = 3;
    const unsigned int NUM_RUNS = SIZE_RATIO;
    // ... other related constants
}

struct KVpair{
    int key;
    int value;
    bool del;
};

bool compareKVpair(KVpair pair1, KVpair pair2);

class Buffer{
public:
    unsigned int size = 0;
    KVpair data[parameters::BUFFER_CAPACITY];
    bool put(int key, int value);
    int get(int key, int& value);
    bool del(int key);
    void sort();
    bool range(int low, int high, std::vector<KVpair> &res);
};


class Layer{
    //KVpair *runs[parameters::NUM_RUNS];
    std::string runs[parameters::NUM_RUNS];
    unsigned int current_run = 0;
    int rank = 0;
    
public:
    std::string get_name(int nthRun);
    int run_size[parameters::NUM_RUNS] = {0};
    void reset();
    int get(int key, int& value);
    bool del(int key);
    bool range(int low, int high, std::vector<KVpair> *res);
    std::string merge(int& size);
    bool add_run_from_buffer(Buffer &buffer);
    bool addRun(std::string run, int size);
    void set_rank(int r);
    
    
};
#endif /* LSM_hpp */
