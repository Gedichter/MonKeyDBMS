//
//  main.cpp
//  LSM_Tree
//
//  Created by Shiyu Huang on 2/21/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#include <iostream>
#include "LSM.hpp"
#include "Tree.hpp"

void buffer_test(){
    Buffer *my_buffer = new Buffer();
    my_buffer->put(1,1);
    my_buffer->put(2,2);
    my_buffer->put(3,3);
    my_buffer->put(2,4);
    my_buffer->del(3);
    
    int query = NULL;
    std::vector<KVpair> range;
    if(my_buffer->get(1, query)){
        std::cout << "The value is " <<query<<std::endl;
    }else{
        std::cout << "Point lookup not found "<<std::endl;
    }
    if(my_buffer->range(2,4,range)){
        std::cout << "The kvpair in the range are " <<std::endl;
        for(int i = 0; i < range.size(); i++){
            std::cout << (range.at(i)).key <<" "<<(range.at(i)).value<<std::endl;
        }
    }else{
        std::cout << "range lookup not found "<<std::endl;
    }
}

void buffer_test2(){
    Buffer my_buffer;
    my_buffer.put(1,1);
    my_buffer.put(2,2);
    my_buffer.put(3,3);
    my_buffer.put(2,4);
    my_buffer.del(3);
    int query = NULL;
    std::vector<KVpair> range;
    if(my_buffer.get(1, query)){
        std::cout << "The value is " <<query<<std::endl;
    }else{
        std::cout << "Point lookup not found "<<std::endl;
    }
    if(my_buffer.range(2,4,range)){
        std::cout << "The kvpair in the range are " <<std::endl;
        for(int i = 0; i < range.size(); i++){
            std::cout << (range.at(i)).key <<" "<<(range.at(i)).value<<std::endl;
        }
    }else{
        std::cout << "range lookup not found "<<std::endl;
    }
}

void merge_test(){
    Buffer my_buffer;
    my_buffer.put(4,8);
    my_buffer.put(2,4);
    my_buffer.put(1,90);
    Layer my_layer;
    my_buffer.sort();
    my_layer.add_run_from_buffer(my_buffer);
    my_buffer.put(4,5);
    my_buffer.put(5,8);
    my_buffer.put(20,9);
    my_buffer.sort();
    my_layer.add_run_from_buffer(my_buffer);
    int size;
    KVpair* run = my_layer.merge(size);
    std::cout<<"f";
}


void tree_test(){
    Tree my_tree;
    for(int i = 0; i < 400; i++){
        my_tree.put(i, i-1);
    }
    for(int i = 0; i < 400; i+=2){
        my_tree.put(i, i);
    }
    for(int i = 0; i < 100; i+=1){
        my_tree.del(i);
    }
    for(int i = 0; i < 50; i++){
        my_tree.put(i, i+5);
    }
    int query = NULL;
    for(int i = 40; i < 60; i++){
        if(my_tree.get(i, query)){
            std::cout << "The new value is " <<query<<std::endl;
        }else{
            std::cout << "Point lookup not found "<<std::endl;
        }
    }
    for(int i = 100; i < 120; i++){
        if(my_tree.get(i, query)){
            std::cout << "The new value is " <<query<<std::endl;
        }else{
            std::cout << "Point lookup not found "<<std::endl;
        }
    }


}

int main(int argc, const char * argv[]) {
    //buffer_test();
    //buffer_test2();
    tree_test();
    //merge_test();
}


