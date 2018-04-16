//
//  main.cpp
//  LSM_Tree
//
//  Created by Shiyu Huang on 2/21/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#include <iostream>
#include <string>
#include <fstream>
#include "LSM.hpp"
#include "Tree.hpp"
#include "Bloom_Filter.hpp"
#include <chrono>

using namespace std::chrono;

void bloomfilter_test(){
    BloomFilter bl = BloomFilter(20000, 0.01);
    for(int i = 0; i < 5000; i+=2){
        bl.add(i);
    }
    bool tp = true;
    for(int i = 0; i < 5000; i+= 2){
        if(!bl.possiblyContains(i)){
            tp = false;
            std::cout<<"false negative!"<<std::endl;
        }
    }
    if(tp){
        std::cout<<"No false negative!"<<std::endl;
    }
    bool tn = true;
    for(int i = 10000; i < 20000; i+=1){
        if(bl.possiblyContains(i)){
            tn = false;
            std::cout<<"false positive!"<<std::endl;
        }
    }
    if(tn){
        std::cout<<"No false positive!"<<std::endl;
    }
    
    bl.reset();
    bool rs = true;
    for(int i = 0; i < 5000; i+= 2){
        if(bl.possiblyContains(i)){
            rs = false;
            std::cout<<"Reset not succeesful!"<<std::endl;
        }
    }
    if(rs){
        std::cout<<"Reset succeesful!"<<std::endl;
    }
}

void read_file(std::string name, unsigned long size){
    std::ifstream file(name, std::ios::binary);
    KVpair* array = new KVpair[size];
    file.read((char*)array, size*sizeof(KVpair));
    for(int i = 0; i < size; i++){
        std::cout<< "one new KV entry"<<std::endl;
        std::cout<< array[i].key <<std::endl;
        std::cout<< array[i].value <<std::endl;
        std::cout<< array[i].del <<std::endl;
    }
}


void create_file(){
    std::string name = "yourFile";
    std::ofstream stream(name.c_str(), std::ios::binary);
    KVpair array[2];
    array[0] = {3,4,true};
    array[1] = {7,8,false};
    stream.write((char*) array, 2*sizeof(KVpair));
    stream.close();
    std::cout<<"finished"<<std::endl;
    read_file(name, 2);
}


void tree_test(){
    Tree my_tree;
    for(int i = 0; i < 4000; i++){
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
//    for(int i = 0; i < my_tree.layers.size();i++){
//        std::cout<<"the layer "<<i<<std::endl;
//        for(int j = 0; j < parameters::NUM_RUNS; j++){
//            if(my_tree.layers.at(i).run_size[j] != 0){
//                std::cout<<"In the file "<<my_tree.layers.at(i).get_name(j)<<std::endl;
//                read_file(my_tree.layers.at(i).get_name(j), my_tree.layers.at(i).run_size[j]);
//            }
//        }
//    }
}

void tree_test_2(){
    Tree my_tree;
    for(int i = 0; i < 10; i++){
        my_tree.put(i, i-1);
    }
    for(int i = 5; i < 15; i+=1){
        my_tree.del(i);
    }
    int query = NULL;
    for(int i = 0; i < 15; i++){
        if(my_tree.get(i, query)){
            std::cout << "The new value is " <<query<<std::endl;
        }else{
            std::cout << "Point lookup not found "<<std::endl;
        }
    }
    //    for(int i = 0; i < my_tree.layers.size();i++){
    //        std::cout<<"the layer "<<i<<std::endl;
    //        for(int j = 0; j < parameters::NUM_RUNS; j++){
    //            if(my_tree.layers.at(i).run_size[j] != 0){
    //                std::cout<<"In the file "<<my_tree.layers.at(i).get_name(j)<<std::endl;
    //                read_file(my_tree.layers.at(i).get_name(j), my_tree.layers.at(i).run_size[j]);
    //            }
    //        }
    //    }
}


//void merge_test_file(){
//    Buffer my_buffer;
//    my_buffer.put(4,8);
//    my_buffer.put(2,4);
//    my_buffer.put(1,90);
//    Layer my_layer;
//    my_buffer.sort();
//    my_layer.add_run_from_buffer(my_buffer);
//    my_buffer.put(4,5);
//    my_buffer.put(5,8);
//    my_buffer.put(20,9);
//    my_buffer.sort();
//    my_layer.add_run_from_buffer(my_buffer);
//    my_buffer.put(8,3);
//    my_buffer.put(12,34);
//    my_buffer.put(21,7);
//    my_buffer.sort();
//    my_layer.add_run_from_buffer(my_buffer);
//    unsigned long size;
//    BloomFilter *bf = NULL;
//    FencePointer *fp = NULL;
//    int num_pointers;
//    std::string run = my_layer.pagewise_merge(size, bf, fp, num_pointers);
//    read_file(run, size);
//}

void main_test(){
    Tree my_tree;
    std::ifstream file ("workload_range.txt");
    char action;
    int key, value;
    
    std::ofstream out("out_range.txt");
    std::streambuf *coutbuf = std::cout.rdbuf(); //save old buf
    std::cout.rdbuf(out.rdbuf()); //redirect std::cout to out.txt!

    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    if (file.is_open()) {
        while (!file.eof()) {
            file >> action;
            if (action == 'p') {
                file >> key;
                file >> value;
                my_tree.put(key, value);
            }
            else if (action == 'g') {
                file >> key;
                int query = NULL;
                if(my_tree.get(key, query)){
                    std::cout <<query<<std::endl;
                }else{
                    std::cout<<std::endl;
                }
            }else if (action == 'd') {
                file >> key;
                my_tree.del(key);
            }else if (action == 'r'){
                int low, high;
                file >> low;
                file >> high;
                std::vector<KVpair> res = my_tree.range(low, high);
                for(int i = 0; i < res.size(); i++){
                    std::cout << res.at(i).key << ":"<<res.at(i).value<<" ";
                }
                std::cout<<std::endl;
            }else{
                std::cout << "Error";
            }
        }
        high_resolution_clock::time_point t2 = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>( t2 - t1 ).count();
        std::cout << "-----------------------------------------------------" << std::endl;
        std::cout << "The elapsed time is " << duration << " microseconds" << std::endl;
        file.close();
        //std::cout << "All operations finished"<<std::endl;
    }
}

void range_test(){
    Tree my_tree;
    for(int i = 0; i < 400; i+=2){
        my_tree.put(i, i-1);
    }
    std::vector<KVpair> res = my_tree.range(100, 150);
    for(int i = 0; i < res.size(); i++){
        std::cout << res.at(i).key << ":"<<res.at(i).value<<"  ";
    }
    std::cout<<std::endl;
    
}



int main(int argc, const char * argv[]) {
    //merge_test_file();
    //read_file("run_1_0", 3);
    //read_file("run_1_1", 3);
    //bloomfilter_test();
    //create_file();
    main_test();
    //tree_test();
    //range_test();
}


