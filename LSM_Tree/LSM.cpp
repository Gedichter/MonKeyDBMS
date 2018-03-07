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
#include <fstream>
#include <cmath>

/**
 utility function
 */

bool compareKVpair(KVpair pair1, KVpair pair2){
    return pair1.key < pair2.key;
}

/*
 Create a bloom filter for the run
 @param run the array of the KVpairs in a run
 numBits the filter size in bits:m
 numHashes number of hash functions in a filter:k
 @return the pointer to the bloom filter
 */
BloomFilter* create_bloom_filter(KVpair* run, unsigned long int numEntries, double falPosRate){
    BloomFilter* filter = new BloomFilter(numEntries, falPosRate);
    for(int i = 0; i < numEntries; i++){
        filter->add(run[i].key);
    }
    return filter;
};

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


Layer::Layer(){
    for(int i = 0; i < parameters::NUM_RUNS; i++){
        filters[i] = NULL;
    }
}


void Layer::set_rank(int r){
    rank = r;
}

/**
 Add element in the buffer to the first level of the LSM tree
 
 @param buffer the buffer
 @return when true, the first layer has reached its limit
 */
bool Layer::add_run_from_buffer(Buffer &buffer){
    filters[current_run] = create_bloom_filter(buffer.data, parameters::BUFFER_CAPACITY, parameters::FPRATE0);
    std::string name = get_name(current_run);
    std::ofstream run(name, std::ios::binary);
    runs[current_run] = name;
    run.write((char*)buffer.data, parameters::BUFFER_CAPACITY*sizeof(KVpair));
    run_size[current_run] = parameters::BUFFER_CAPACITY;
    current_run++;
    //TODO: change the setter on buffer
    buffer.size = 0;
    run.close();
    return current_run == parameters::NUM_RUNS;
};

/**
 Reset the layer, free memory
 */
void Layer::reset(){
    current_run = 0;
    for(int i = 0; i < parameters::NUM_RUNS; i++){
        run_size[i] = 0;
        delete filters[i];
        filters[i] = NULL;
        runs[i].clear();
        std::string name = get_name(i);
        if(remove(name.c_str()) != 0){
            std::cout<<"Error deleting the file"<<std::endl;
        };
    }
};

std::string Layer::get_name(int nthRun){
    return "run_" + std::to_string(rank) + "_" + std::to_string(nthRun);
}

/**
 Merge all runs to one run in this level
 NOTE: Can't use heap to do the merge sort because we need to maintain the order of runs to know
 which are the newest values
 Use temp vector to store the merged result then write to file: minimize number of I/O
 @param size stores the size of the resulting run
 @return the name of the file of the new run
 */
std::string Layer::merge(int &size, BloomFilter*& bf){
    KVpair *read_runs[parameters::NUM_RUNS];
    std::vector<KVpair> run_buffer;
    int* indexes = new int[parameters::NUM_RUNS];
    for(int i = 0; i < parameters::NUM_RUNS; i++){
        indexes[i] = 0;
        std::ifstream inStream(get_name(i), std::ios::binary);
        read_runs[i] = new KVpair[run_size[i]];
        inStream.read((char*)read_runs[i], run_size[i]*sizeof(KVpair));
        inStream.close();
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
                if(read_runs[i][indexes[i]].key < min){
                    min = read_runs[i][indexes[i]].key;
                    min_indexes.clear();
                    min_indexes.push_back(i);
                }else if (read_runs[i][indexes[i]].key == min){
                    min_indexes.push_back(i);
                }
            }
        }
        int min_index = min_indexes.back();
        run_buffer.push_back(read_runs[min_index][indexes[min_index]]);
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
    //write to file
    size = run_buffer.size();
    std::string name = "run_" + std::to_string(rank) + "_temp";
    std::ofstream new_file(name, std::ios::binary);
    KVpair* new_run = new KVpair[size];
    std::copy(run_buffer.begin(), run_buffer.end(), new_run);
    new_file.write((char*)new_run, size*sizeof(KVpair));
    new_file.close();
    
    //create bloom filter
    if(rank < parameters::LEVELWITHBF-1){
        bf = new BloomFilter(size, parameters::FPRATE0*pow(parameters::SIZE_RATIO, rank));
        for(int i = 0; i < size; i++){
            bf->add(new_run[i].key);
        }
    }
    
    //reset the layer, free the dynamic memory
    reset();
    delete[] indexes;
    for(int i = 0; i < parameters::NUM_RUNS; i++){
        delete [] read_runs[i];
    }
    delete [] new_run;
    
    return name;
};

/**
 Add new run from the previous level of the LSM tree
 
 @param run the pointer to the new run
 size the size of the new run
 @return when true, the layer has reached its limit
 */
bool Layer::add_run(std::string run, int size, BloomFilter* bf){
    std::string newName = get_name(current_run);
    if(rename(run.c_str(), newName.c_str()) != 0){
        std::cout << "rename failed"<<std::endl;
    };
    runs[current_run] = newName;
    run_size[current_run] = size;
    filters[current_run] = bf;
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
        if(rank >= parameters::LEVELWITHBF){
            int c = check_run(key, value, i);
            if(c!=0) return c;
        }else{
            if(filters[i]->possiblyContains(key)){
                int c = check_run(key, value, i);
                if(c!=0) return c;
            }
        }
    }
    return 0;
};

int Layer::check_run(int key, int& value, int index){
    int cap = run_size[index];
    KVpair* curRun = new KVpair[cap];
    std::ifstream inStream(get_name(index), std::ios::binary);
    inStream.read((char *)curRun, cap*sizeof(KVpair));
    inStream.close();
    for(int j = 0; j < cap; j++){
        if(curRun[j].key == key){
            if(curRun[j].del){
                return -1;
            }else{
                value = curRun[j].value;
                return 1;
            }
        }
    }
    delete[] curRun;
    return 0;
}


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

