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

Tree::Tree(){
    Layer layer;
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
    int size = 0;
    KVpair *new_run = low.merge(size);
    return high.addRun(new_run, size);
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
            //TODO: change to getter function
            Layer layer;
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

void Tree::del(int key){
    if(buffer.del(key)){
        flush();
    }
};

