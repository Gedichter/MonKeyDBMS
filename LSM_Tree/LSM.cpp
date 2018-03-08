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

/*
 Create an array of fence pointer for the run
 Called only when the size of the run is greater than parameters::KVPAIRPERPAGE
 @param run is the array of the KVpairs in a run
 size is the length of the run
 num_pointers stores the number of fence pointers in the array
 @return the pointer to the array
 */
FencePointer* create_fence_pointer(KVpair* run, unsigned long int size, int& num_pointers){
    num_pointers = (int)ceil((double)size/(double)parameters::KVPAIRPERPAGE);
    FencePointer* fparray = new FencePointer[num_pointers];
    for(int i = 0; i < num_pointers; i++){
        fparray[i].min = run[i*parameters::KVPAIRPERPAGE].key;
        fparray[i].max = run[std::min((i+1)*parameters::KVPAIRPERPAGE-1, size-1)].key;
    }
    return fparray;
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
    //Bloom filter
    filters[current_run] = create_bloom_filter(buffer.data, parameters::BUFFER_CAPACITY, parameters::FPRATE0);
    //Fence pointer
    if(parameters::BUFFER_CAPACITY > parameters::KVPAIRPERPAGE){
        int numPointers = 0;
        pointers[current_run] = create_fence_pointer(buffer.data, parameters::BUFFER_CAPACITY, numPointers);
        pointer_size[current_run] = numPointers;
    }
    //write to file
    std::string name = get_name(current_run);
    std::ofstream run(name, std::ios::binary);
    runs[current_run] = name;
    run.write((char*)buffer.data, parameters::BUFFER_CAPACITY*sizeof(KVpair));
    run_size[current_run] = parameters::BUFFER_CAPACITY;
    current_run++;
    run.close();
    //TODO: change the setter on buffer
    buffer.size = 0;
    return current_run == parameters::NUM_RUNS;
};


/**
 Reset the layer, free memory, delete file
 */
void Layer::reset(){
    current_run = 0;
    for(int i = 0; i < parameters::NUM_RUNS; i++){
        run_size[i] = 0;
        pointer_size[i] = 0;
        delete filters[i];
        if(pointers[i] != NULL){
            delete [] pointers[i];
            pointers[i] = NULL;
        }
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
std::string Layer::merge(int &size, BloomFilter*& bf, FencePointer*& fp, int &num_pointers){
    //read files and set index
    KVpair *read_runs[parameters::NUM_RUNS];
    int* indexes = new int[parameters::NUM_RUNS];
    for(int i = 0; i < parameters::NUM_RUNS; i++){
        indexes[i] = 0;
        std::ifstream inStream(get_name(i), std::ios::binary);
        read_runs[i] = new KVpair[run_size[i]];
        inStream.read((char*)read_runs[i], run_size[i]*sizeof(KVpair));
        inStream.close();
    }
    //perform merge
    std::vector<KVpair> run_buffer;
    int ct = parameters::NUM_RUNS; //the count of active arrays
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
    //set the new size, create array
    size = run_buffer.size();
    KVpair* new_run = new KVpair[size];
    //write to file
    std::string name = "run_" + std::to_string(rank) + "_temp";
    std::ofstream new_file(name, std::ios::binary);
    std::copy(run_buffer.begin(), run_buffer.end(), new_run);
    new_file.write((char*)new_run, size*sizeof(KVpair));
    new_file.close();
    //create bloom filter TODO: change to create_bloom_filter function!!
    if(rank < parameters::LEVELWITHBF-1){
        bf = new BloomFilter(size, parameters::FPRATE0*pow(parameters::SIZE_RATIO, rank));
        for(int i = 0; i < size; i++){
            bf->add(new_run[i].key);
        }
    }
    //create fence pointer
    if(size > parameters::KVPAIRPERPAGE){
        fp = create_fence_pointer(new_run, size, num_pointers);
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
bool Layer::add_run(std::string run, int size, BloomFilter* bf, FencePointer* fp, int num_pointers){
    std::string newName = get_name(current_run);
    if(rename(run.c_str(), newName.c_str()) != 0){
        std::cout << "rename failed"<<std::endl;
    };
    runs[current_run] = newName;
    run_size[current_run] = size;
    filters[current_run] = bf;
    pointers[current_run] = fp;
    pointer_size[current_run] = num_pointers;
    current_run += 1;
    return current_run == parameters::NUM_RUNS;
}


/**
 a basic implementation of get
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
    //indexes for the fence pointer
    unsigned long int offset = 0;
    bool valid = false;
    //check the fence pointer
    if(pointers[index] != NULL){
        for(int i = 0; i < pointer_size[index]; i++){
            if(key>= pointers[index][i].min && key <= pointers[index][i].max){
                offset = i*parameters::KVPAIRPERPAGE;
                valid = true;
                break;
            }
        }
        if(!valid) return 0;
    }
    //read the needed page from the file
    int read_size = run_size[index];
    if(valid){
        read_size = std::min(parameters::KVPAIRPERPAGE, (run_size[index]-offset));
    }
    KVpair* curRun = new KVpair[read_size];
    std::ifstream inStream(get_name(index), std::ios::binary);
    inStream.seekg(offset*sizeof(KVpair));
    inStream.read((char *)curRun, read_size*sizeof(KVpair));
    inStream.close();
    //TODO: change to binary search
    for(int j = 0; j < read_size; j++){
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

