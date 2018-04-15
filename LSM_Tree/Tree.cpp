//
//  Tree.cpp
//  LSM_Tree
//
//  Created by Shiyu Huang on 2/22/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#include "Tree.hpp"
#include "LSM.hpp"
#include <cmath>
#include <unordered_map>

Tree::Tree(){
    Layer layer;
    layer.set_rank(0);
    layers.push_back(layer);
}

/**
 flush buffer to the LSM tree
 
 @return when true, the first layer has reached its limit
 */
bool Tree::bufferFlush(){
    buffer.sort();
    return layers[0].add_run_from_buffer(buffer);
}

/**
 flush level in the LSM tree
 
 @param low layer to be flushed
 high layer to be flushed in
 @return when true, the high layer has reached its limit
 */
bool Tree::layerFlush(Layer &low, Layer &high){
    int num_pointers = 0;
    unsigned long size = 0;
    BloomFilter *bf = NULL;
    FencePointer *fp = NULL;
    std::string new_run = low.merge(size, bf, fp, num_pointers);
    return high.add_run(new_run, size, bf, fp, num_pointers);
};

void Tree::flush(){
    if(bufferFlush()){
        int level = 0;
        bool goOn = true;
        while(goOn && level + 1 < layers.size()){
            goOn = layerFlush(layers.at(level), layers.at(level+1));
            level += 1;
        }
        if(goOn){
            Layer layer;
            layer.set_rank(layers.size());
            layers.push_back(layer);
            layerFlush(layers.at(level), layers.at(level+1));
        }
    }

}

void Tree::put(int key, int value){
    if(buffer.put(key, value)){
        flush();
    }
};

bool Tree::get(int key, int& value){
    switch (buffer.get(key, value)) {
        case 1:
            return true;
        case -1:
            return false;
        default:
            for(int i = 0; i < layers.size(); i++){
                switch (layers.at(i).get(key, value)) {
                    case 1:
                        return true;
                    case -1:
                        return false;
                    default:
                        break;
                }
            }
    }
    return false;
};


/**
 return the all the key value pairs within the range
 @params low : include
 high : not include
 return vector of the key-value pair
 */
std::vector<KVpair> Tree::range(int low, int high){
    std::unordered_map<int, KVpair> result_buffer;
    buffer.range(low, high, result_buffer);
    for(int i = 0; i < layers.size(); i++){
        layers.at(i).range(low, high, result_buffer);
    }
    std::vector<KVpair> result;
    for (auto const& x : result_buffer)
    {
        KVpair kv = x.second;
        if(!kv.del){
            result.push_back(kv);
        }
    }
    return result;
};

void Tree::del(int key){
    if(buffer.del(key)){
        flush();
    }
};

