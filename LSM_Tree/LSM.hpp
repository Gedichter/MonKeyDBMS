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

namespace parameters
{
    const unsigned int BUFFER_CAPACITY = 3;
    const unsigned int SIZE_RATIO = 2;
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
    Buffer();
    bool put(int key, int value);
    int get(int key, int& value);
    bool del(int key);
    void sort();
    bool range(int low, int high, std::vector<KVpair> &res);
};

class Layer{
    int* run_size;
    KVpair **runs;
    unsigned int current_run;
    
public:
    unsigned int num_runs;
    void reset();
    Layer(unsigned int num_runs);
    int get(int key, int& value);
    bool del(int key);
    bool range(int low, int high, std::vector<KVpair> *res);
    KVpair* merge(int& size);
    bool add_run_from_buffer(Buffer &buffer);
    bool addRun(KVpair *run, int size);
    
    
};
#endif /* LSM_hpp */
