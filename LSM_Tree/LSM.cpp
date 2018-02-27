//
//  LSM.cpp
//  LSM_Tree
//
//  Created by Shiyu Huang on 2/21/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#include <iostream>
#include "LSM.hpp"
#include <vector>
#include <algorithm>
#include <assert.h>
#include <climits>

/**
 utility function
 */

//TODO change the sort so that can use reference type
bool compareKVpair(KVpair pair1, KVpair pair2){
    return pair1.key < pair2.key;
}

/** Buffer
 */


/**
 Put the value associated with the key in the buffer
 @param
 key the key to insert
 value the value to insert
 @return when true, the buffer has reached capacity
 */
bool Buffer::put(int key, int value){
    bool exist = false;
    for(int i = 0; i < size; i++){
        if(data[i].key == key){
            data[i].value = value;
            data[i].del = false;
            exist = true;
        }
    }
    if(!exist){
        data[size].key = key;
        data[size].value = value;
        data[size].del = false;
        size += 1;
        if(size >= parameters::BUFFER_CAPACITY){
            return true;
        }
    }
    return false;
};


/**
Get the value associated with the key in the buffer
 
 @param key the key to delete
 value: address to store the return value
 @return 1: found
 0: not found
 -1: (latest version)deleted, which means no need to go on searching
 */
int Buffer::get(int key, int& value){
    for(int i = size-1; i >= 0; i--){
        if(data[i].key == key){
            if(data[i].del){
                return -1;
            }else{
                value = data[i].value;
                return 1;
            }
        }
    }
    return 0;
};

/**
 Delete the value associated with the key in the buffer
 
 @param key the key to delete
 @return when true, the buffer has reached capacity
 */
bool Buffer::del(int key){
    for(int i = 0; i < size; i++){
        if(data[i].key == key){
            data[i].del = true;
            return false;
        }
    }
    //when not found in the buffer
    data[size].key = key;
    data[size].value = 0;
    data[size].del = true;
    size += 1;
    if(size >= parameters::BUFFER_CAPACITY) return true;
    return false;
};

bool Buffer::range(int low, int high, std::vector<KVpair> &res){
    bool found = false;
    for(int i = 0; i < size; i++){
        if(data[i].key < high && data[i].key >= low && data[i].del == false){
            res.push_back(data[i]);
            found = true;
        }
    }
    return found;
}

void Buffer::sort(){
    std::sort(data, data+parameters::BUFFER_CAPACITY, compareKVpair);
};

/**
 Layer
 */

/**
 Add element in the buffer to the first level of the LSM tree
 
 @param buffer the buffer
 @return when true, the first layer has reached its limit
 */
bool Layer::add_run_from_buffer(Buffer &buffer){
    runs[current_run] = new KVpair[parameters::BUFFER_CAPACITY];
    run_size[current_run] = parameters::BUFFER_CAPACITY;
    for(int i = 0; i < parameters::BUFFER_CAPACITY;i++){
        runs[current_run][i].key = buffer.data[i].key;
        runs[current_run][i].value = buffer.data[i].value;
        runs[current_run][i].del = buffer.data[i].del;
    }
    current_run++;
    //TODO: change the setter on buffer
    buffer.size = 0;
    return current_run == parameters::NUM_RUNS;
};

/**
 Reset the layer, free memory
 */
void Layer::reset(){
    current_run = 0;
    for(int i = 0; i < parameters::NUM_RUNS; i++){
        run_size[i] = 0;
        delete[] runs[i];
    }
};


/**
 Merge all runs to one run in this level
 NOTE: Can't use heap to do the merge sort because we need to maintain the order of runs to know
 which are the newest values
 @param size stores the size of the resulting run
 @return the pointer to the new run
 */
KVpair* Layer::merge(int &size){
    std::vector<KVpair> run_buffer;
    int * indexes = new int[parameters::NUM_RUNS];
    for(int i = 0; i < parameters::NUM_RUNS; i++){
        indexes[i] = 0;
    }
    //count the number of active arrays
    int ct = parameters::NUM_RUNS;
    int min;
    while(ct > 0){
        std::vector<int> min_indexes;
        min = INT_MAX;
        //there is no duplicate inside each run, scan from the old runs to the new runs
        for(int i = 0; i < parameters::NUM_RUNS; i++){
            if(indexes[i] >= 0){
                if(runs[i][indexes[i]].key < min){
                    min = runs[i][indexes[i]].key;
                    min_indexes.clear();
                    min_indexes.push_back(i);
                }else if (runs[i][indexes[i]].key == min){
                    min_indexes.push_back(i);
                }
            }
        }
        int min_index = min_indexes.back();
        run_buffer.push_back(runs[min_index][indexes[min_index]]);
        for(int i = 0; i < min_indexes.size(); i++){
            int cur_index = min_indexes.at(i);
            indexes[cur_index] += 1;
            if(indexes[cur_index] >= run_size[cur_index]){
                //set index to -1 when the the last element of the array is used
                indexes[cur_index] = -1;
                ct -= 1;
            }
        }
    }
    //reset the layer, free the memory
    reset();
    size = run_buffer.size();
    KVpair *new_run = new KVpair[size];
    //write the sorted vector to array
    std::copy(run_buffer.begin(), run_buffer.end(), new_run);
    delete[] indexes;
    return new_run;
};

/**
 Add new run from the previous level of the LSM tree
 
 @param run the pointer to the new run
 size the size of the new run
 @return when true, the layer has reached its limit
 */
bool Layer::addRun(KVpair *run, int size){
    runs[current_run] = run;
    run_size[current_run] = size;
    current_run += 1;
    return current_run == parameters::NUM_RUNS;
}


/**
 a basic implementation of get
 TODO: change to binary search, Bloom filter + fence pointer
 @return 1: found
 0: not found
 -1: (latest version)deleted, which means no need to go on searching
 */
int Layer::get(int key, int& value){
    for(int i = current_run-1; i >= 0; i--){
        int cap = run_size[i];
        for(int j = 0; j < cap; j++){
            if(runs[i][j].key == key){
                if(runs[i][j].del){
                    return -1;
                }else{
                    value = runs[i][j].value;
                    return 1;
                }
            }
        }
    }
    return 0;
};


bool range(int low, int high, std::vector<KVpair> *res);




















//struct MinHeapNode
//{
//    //values of the
//    int key;
//    int value;
//    bool del;
//    int i; // index of the array from which the element is taken
//    int j; // index of the next element to be picked from array
//};
//
//static bool heapComp(MinHeapNode *a, MinHeapNode *b) {
//    return a->value > b->value;
//}
//
//MinHeapNode *createNode(int key, int value, bool del, int i, int j){
//    MinHeapNode *node = new MinHeapNode;
//    node->key = key;
//    node->value = value;
//    node->del = del;
//    node->i = i;
//    node->j = j;
//    return node;
//}

