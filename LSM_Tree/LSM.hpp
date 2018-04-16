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
#include <unordered_map>
#include "Bloom_Filter.hpp"
#include <math.h>

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
    const unsigned int BUFFER_CAPACITY = 1024;
    const unsigned int SIZE_RATIO = 4;
    const unsigned int NUM_RUNS = SIZE_RATIO;
    const double FPRATE0 = 0.001;
    /*
     reference: https://apple.stackexchange.com/questions/78802/what-are-the-sector-sizes-on-mac-os-x
     Unit: Bytes
     */
    const unsigned long int KVPAIRPERPAGE = 4096/sizeof(KVpair);
    const double FPTHRESHOLD = 0.8;
    const int LEVELWITHBF = (int)log(FPTHRESHOLD/FPRATE0)/log(parameters::SIZE_RATIO);
    
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
    void range(int low, int high, std::unordered_map<int, KVpair>& res);
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
    void range(int low, int high, std::unordered_map<int, KVpair>& range_buffer);
    std::string merge(unsigned long &size, BloomFilter*& bf, FencePointer*& fp, int &num_pointers);
    std::string pagewise_merge(unsigned long &size, BloomFilter*& bf, FencePointer*& fp, int &num_pointers);
    bool add_run_from_buffer(Buffer &buffer);
    bool add_run(std::string run, unsigned long size, BloomFilter* bf, FencePointer* fp, int num_pointers);
    void set_rank(int r);
    void range_run(int low, int high, std::unordered_map<int, KVpair>& range_buffer, int index);
    
};
#endif /* LSM_hpp */
