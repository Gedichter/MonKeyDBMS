//
//  Tree.hpp
//  LSM_Tree
//
//  Created by Shiyu Huang on 2/22/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#ifndef Tree_hpp
#define Tree_hpp

#include <stdio.h>
#include "LSM.hpp"
#include <vector>

class Tree{
    Buffer buffer;

public:
    std::vector<Layer> layers;
    Tree();
    void flush();
    bool bufferFlush();
    bool layerFlush(Layer &low, Layer &high);
    void put(int key, int value);
    bool get(int key, int& value);
    void del(int key);
    std::vector<KVpair> range(int low, int high);
    
};

#endif /* Tree_hpp */
