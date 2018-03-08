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
#include "Bloom_Filter.hpp"

struct KVpair{
    int key;
    int value;
    bool del;
};

bool compareKVpair(KVpair pair1, KVpair pair2);

struct FencePointer{
    int min;
    int max;
};

namespace parameters
{
    const unsigned int BUFFER_CAPACITY = 3;
    const unsigned int SIZE_RATIO = 3;
    const unsigned int NUM_RUNS = SIZE_RATIO;
    const double FPRATE0 = 0.001;
    /*
     reference: https://apple.stackexchange.com/questions/78802/what-are-the-sector-sizes-on-mac-os-x
     Unit: Bytes
     */
    const unsigned long int KVPAIRPERPAGE = 64/sizeof(KVpair);
    const int LEVELWITHBF = 6;
    // ... other related constants
}


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

BloomFilter* create_bloom_filter(KVpair* run, unsigned long int numEntries, double falPosRate);

class Layer{
    std::string runs[parameters::NUM_RUNS];
    unsigned int current_run = 0;
    int rank = 0;
    BloomFilter *filters[parameters::NUM_RUNS];
    FencePointer *pointers[parameters::NUM_RUNS]  = {NULL};
    int pointer_size[parameters::NUM_RUNS] = {0};
    
public:
    unsigned long int run_size[parameters::NUM_RUNS] = {0};
    
    
    Layer();
    std::string get_name(int nthRun);
    void reset();
    int get(int key, int& value);
    int check_run(int key, int& value, int i);
    bool del(int key);
    bool range(int low, int high, std::vector<KVpair> *res);
    std::string merge(int &size, BloomFilter*& bf, FencePointer*& fp, int &num_pointers);
    bool add_run_from_buffer(Buffer &buffer);
    bool add_run(std::string run, int size, BloomFilter* bf, FencePointer* fp, int num_pointers);
    void set_rank(int r);
    
};
#endif /* LSM_hpp */
